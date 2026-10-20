"""The analysis store and the analysis queue.

Shared by server.py (which accepts requests and answers status polls) and
worker.py (which runs the analyses). Nothing here imports Flask or the
pipeline, so the worker can import it without a web stack and the server
can import it without touching GDAL.

Layout under RUNS_DIR
---------------------
    <analysis_id>/job.json          the request, validated and parsed, as queued
    <analysis_id>/status.json       the worker's progress while it runs
    <analysis_id>/manifest.json     the completed record (see save_result)
    <analysis_id>/contours.geojson
    <analysis_id>/failed.json       why a failed run failed
    <analysis_id>/*.tif             rasters (pruned after 180 days)
    .queue/<analysis_id>.queued     one marker per waiting analysis
    .queue/<analysis_id>.running    the same marker once a worker slot took it
    .queue/worker.json              the worker's heartbeat
    .queue/stats.json               recent run durations, for the estimate

Why a directory of markers and not a database: the gunicorn workers and
the queue worker are separate processes with nothing in common but the
filesystem, and rename(2) on one filesystem is atomic, so a marker can be
claimed by exactly one slot. Scanning `.queue/` costs one readdir per
status poll; the run directories themselves are never scanned to answer
a poll.

Ordering: analysis_ids begin with a UTC stamp, so sorting the marker
names sorts the queue by submission time.
"""
import json
import math
import os
import re
import secrets
import time
import traceback
from datetime import datetime, timezone

APP_VERSION = '1.19'

# ============================================================
# The analysis store
# ------------------------------------------------------------
# Every analysis gets its own directory under RUNS_DIR, named by its
# analysis_id, holding the rasters the pipeline wrote, contours.geojson
# and manifest.json (inputs, settings, data-source versions, timings).
#
# Before v1.18 rasters went to one mkdtemp directory per gunicorn worker
# with fixed file names, so the next analysis handled by the same worker
# overwrote the previous one's rasters while the previous record still
# pointed at them (its downloads then served the wrong raster under the
# right name), and the records themselves sat in /tmp keyed by rounded
# coordinates, so two analyses from one IPP replaced each other.
#
# RUNS_DIR sits beside app/ and cache/, outside the rsync --delete tree,
# so a deploy cannot touch it. tools/prune_runs.py removes rasters after
# a retention window; manifests and contours are kept, and the nightly
# backup carries them (weleber-server-config/scripts/weleber-backup).
# ============================================================
RUNS_DIR = os.environ.get('WISAR_RUNS_DIR', '/var/www/sar.weleber.net/runs')
QUEUE_DIR = os.path.join(RUNS_DIR, '.queue')
try:
    os.makedirs(QUEUE_DIR, exist_ok=True)
    # Per-process name: the worker starts two children in the same second,
    # and with one fixed name the second's remove() found the first had
    # already removed it and the child exited (seen in the fork, 2026-10-19).
    _probe = os.path.join(RUNS_DIR, f'.write-test.{os.getpid()}')
    with open(_probe, 'w') as _f:
        _f.write('ok')
    try:
        os.remove(_probe)
    except FileNotFoundError:
        pass
except OSError as _e:
    # Refuse to start rather than fall back to /tmp: a silent fallback is
    # exactly the failure this store exists to end, and a service that
    # does not start fails the deploy's health check loudly.
    raise SystemExit(f"ERROR: analysis store {RUNS_DIR} is not writable ({_e}). "
                     f"Set WISAR_RUNS_DIR to a writable directory.")

# analysis_id doubles as a directory name and a URL segment; anything
# outside this set is answered 404 before it reaches the filesystem.
ANALYSIS_ID_RE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$')

# Raster keys in a pipeline result -> file name inside the run directory.
RASTER_FILES = {
    'dem_path': 'dem.tif',
    'nlcd_path': 'nlcd.tif',
    'cost_surface_path': 'cost_surface.tif',
    'cost_distance_path': 'cost_distance.tif',
    'probability_path': 'probability.tif',
    'jacobs_masks_path': 'jacobs_masks.tif',
}
MANIFEST_NAME = 'manifest.json'
CONTOURS_NAME = 'contours.geojson'
FAILED_NAME = 'failed.json'
JOB_NAME = 'job.json'
STATUS_NAME = 'status.json'

# ============================================================
# Queue parameters
# ------------------------------------------------------------
# SLOTS: analyses the worker runs at once. The Linode has four cores and
# serves nine other sites (and the canyons worker, which may take two
# cores for a quarter of an hour); a cost-distance run is a
# single-threaded pure-Python loop, so two at a time leaves the web
# services responsive and a third would mostly slow the other two. Raise
# it with WISAR_WORKER_SLOTS in the unit, not here. The queue, not more
# slots, is what absorbs a room full of people pressing Run at once.
# MAX_QUEUE: beyond this the server answers 503 instead of queueing;
# forty analyses is a wait of about twenty minutes, and anything deeper
# is either an outage or abuse.
# WORKER_STALE_S: a heartbeat older than this means the worker is not
# running, and status polls say so instead of promising an estimate.
# ============================================================
SLOTS = int(os.environ.get('WISAR_WORKER_SLOTS', '2'))
MAX_QUEUE = int(os.environ.get('WISAR_MAX_QUEUE', '40'))
WORKER_STALE_S = 30
# Used for the estimate until the worker has recorded real durations.
# A Hiker run on the Linode took 43 s end to end in October 2026.
DEFAULT_DURATION_S = 60.0
STATS_KEEP = 30

# Pipeline steps in the order the orchestrators mark them, with the
# message the browser shows while that step runs. The worker reports
# the *next* step after each mark. 'probability' only occurs in TARR mode.
STAGES = [
    ('dem', 'Downloading elevation data (USGS 3DEP)...'),
    ('nlcd', 'Loading land cover (NLCD snapshot)...'),
    ('osm', 'Loading trails, roads, power lines and waterways (OSM snapshot)...'),
    ('nhd', 'Loading hydrography (NHD snapshot)...'),
    ('cost_surface', 'Building the terrain friction surface...'),
    ('cost_distance', 'Running anisotropic cost-distance from the IPP (the slow step)...'),
    ('jacobs_masks', 'Computing terrain-attractor masks...'),
    ('probability', 'Generating the probability surface...'),
    ('contours', 'Extracting contours...'),
    ('save', 'Saving the result...'),
]
STAGE_INDEX = {name: i for i, (name, _) in enumerate(STAGES)}


def utc_now_iso():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def clean_label(value):
    """Optional incident label from the request: whitespace-normalised, capped."""
    return ' '.join(str(value or '').split())[:120]


def new_analysis_id(mode, lat, lng):
    """Unique, sortable, readable id: time, mode, IPP, random suffix.

    The suffix makes the id unguessable, so a link to an analysis is the
    credential for opening it (the site has no login), and two analyses
    started in the same second from the same IPP get separate directories.
    """
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    return f"{stamp}_{mode}_{lat:.4f}_{lng:.4f}_{secrets.token_hex(4)}"


def run_dir(analysis_id):
    """Directory for an analysis_id, or None if the id is malformed."""
    if not analysis_id or not ANALYSIS_ID_RE.match(analysis_id):
        return None
    return os.path.join(RUNS_DIR, analysis_id)


def write_json_atomic(path, obj):
    """Write via a temp file and rename, so a reader never sees a partial file."""
    tmp = path + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(obj, f, indent=1)
    os.replace(tmp, path)


def read_json(path, default=None):
    """A JSON file, or `default` if it is missing or being rewritten."""
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def snapshot_versions(warnings):
    """Which data-source versions produced this run, for the manifest.

    A re-run later is not the same analysis: the OSM snapshot changes
    weekly, NHD quarterly, NLCD yearly, and the DEM may have come from the
    staged tiles instead of the ImageServer. Recording this is what lets
    a result be explained after the fact.
    """
    from pipeline import osm_cache, nlcd_cache, nhd_cache
    out = {}
    for name, mod in (('osm', osm_cache), ('nlcd', nlcd_cache), ('nhd', nhd_cache)):
        try:
            meta = mod.read_cache_metadata() if mod.cache_is_available() else {}
        except Exception:
            meta = {}
        out[name] = {k: meta[k] for k in ('built_at', 'year', 'source') if k in meta}
    dem_fallback = any(w.get('source') == 'dem' and w.get('severity') == 'info'
                       for w in warnings or [])
    out['dem'] = 'usgs_staged_tiles' if dem_fallback else '3dep_imageserver'
    return out


def raster_summary(path):
    """Bounds and grid of the raster every other output derives from."""
    import rasterio
    with rasterio.open(path) as src:
        b = src.bounds
        t = src.transform
        return ({'west': b.left, 'south': b.bottom, 'east': b.right, 'north': b.top},
                {'width': src.width, 'height': src.height,
                 'cell_m': round(abs(t.e) * 110540, 1)})


def save_result(analysis_id, result, mode, request_data, label, started_at, extra):
    """Write contours.geojson and manifest.json into the run directory.

    `result` is the pipeline's return dict (absolute raster paths, contour
    GeoJSON, warnings, timings). The manifest records file names relative
    to its directory, so the store can be moved or restored elsewhere.
    `extra` carries the mode-specific fields (percentiles, profile, or
    isochrone_params) and the IPP as floats. The manifest is the same
    record the synchronous endpoint wrote before the queue (v1.18).
    """
    d = run_dir(analysis_id)
    bounds, grid = raster_summary(result['cost_distance_path'])
    files = {}
    for key, name in RASTER_FILES.items():
        p = result.get(key)
        if p and os.path.exists(p):
            files[key[:-5]] = name          # 'dem_path' -> 'dem'
    contours = result.get('contour_geojson') or {'type': 'FeatureCollection', 'features': []}
    write_json_atomic(os.path.join(d, CONTOURS_NAME), contours)
    manifest = {
        'schema': 1,
        'analysis_id': analysis_id,
        'created_utc': started_at,
        'completed_utc': utc_now_iso(),
        'app_version': APP_VERSION,
        'mode': mode,
        'label': label,
        # The request as the browser sent it: calibrated percentiles, the
        # raw Koester values and multipliers behind them, profile, speed...
        'request': request_data,
        'radius_km': result.get('radius_km'),
        'bbox': list(result.get('bbox') or []),
        'bounds': bounds,
        'grid': grid,
        'warnings': result.get('warnings', []),
        'sources': snapshot_versions(result.get('warnings')),
        'timings_s': result.get('timings_s', {}),
        'files': files,
        'contours': CONTOURS_NAME,
        'contour_count': len(contours.get('features', [])),
    }
    manifest.update(extra)
    write_json_atomic(os.path.join(d, MANIFEST_NAME), manifest)
    return manifest


def record_failure(analysis_id, mode, request_data, label, started_at, error, tb=None):
    """Leave failed.json in the run directory so a failed run is explainable."""
    d = run_dir(analysis_id)
    if not d:
        return
    try:
        os.makedirs(d, exist_ok=True)
        write_json_atomic(os.path.join(d, FAILED_NAME), {
            'analysis_id': analysis_id,
            'created_utc': started_at,
            'failed_utc': utc_now_iso(),
            'app_version': APP_VERSION,
            'mode': mode,
            'label': label,
            'request': request_data,
            'error': str(error),
            'traceback': tb if tb is not None else traceback.format_exc(),
        })
    except Exception as e:
        print(f"  WARNING: could not record failure for {analysis_id}: {e}")


def load_result(analysis_id):
    """Read a run back from its manifest.

    Returns a dict shaped like the pipeline result the PNG and download
    routes consume (absolute `*_path` keys, `contour_geojson`,
    `percentiles`, `isochrone_params`), plus every manifest field. None if
    the id is malformed or the run does not exist. A pruned raster leaves
    its `*_path` pointing at a missing file; the routes check existence.
    """
    d = run_dir(analysis_id)
    if not d or not os.path.isfile(os.path.join(d, MANIFEST_NAME)):
        return None
    with open(os.path.join(d, MANIFEST_NAME)) as f:
        manifest = json.load(f)
    result = dict(manifest)
    files = manifest.get('files', {})
    for key, name in RASTER_FILES.items():
        result[key] = os.path.join(d, name) if key[:-5] in files else None
    result['contours_path'] = os.path.join(d, manifest.get('contours', CONTOURS_NAME))
    result['manifest_path'] = os.path.join(d, MANIFEST_NAME)
    result['contour_geojson'] = None
    if os.path.isfile(result['contours_path']):
        with open(result['contours_path']) as f:
            result['contour_geojson'] = json.load(f)
    return result


# ============================================================
# The queue
# ============================================================

def marker_path(analysis_id, state):
    return os.path.join(QUEUE_DIR, f'{analysis_id}.{state}')


def list_markers():
    """(queued_ids_in_order, running_ids) from one readdir of QUEUE_DIR."""
    queued, running = [], []
    try:
        names = os.listdir(QUEUE_DIR)
    except OSError:
        names = []
    for n in names:
        if n.endswith('.queued'):
            queued.append(n[:-7])
        elif n.endswith('.running'):
            running.append(n[:-8])
    queued.sort()
    running.sort()
    return queued, running


class QueueFull(Exception):
    def __init__(self, depth):
        super().__init__(f'queue is full ({depth} waiting)')
        self.depth = depth


def enqueue(analysis_id, mode, request_data, label, params, extra):
    """Record a validated request and put it in the queue.

    job.json is written first, then the marker, so a worker that sees the
    marker always finds the job. Raises QueueFull when the queue is at
    MAX_QUEUE; the caller turns that into a 503.
    """
    queued, running = list_markers()
    if len(queued) >= MAX_QUEUE:
        raise QueueFull(len(queued))
    d = run_dir(analysis_id)
    os.makedirs(d)
    job = {
        'schema': 1,
        'analysis_id': analysis_id,
        'created_utc': utc_now_iso(),
        'app_version': APP_VERSION,
        'mode': mode,
        'label': label,
        'request': request_data,
        'params': params,
        'extra': extra,
        'attempts': 0,
    }
    write_json_atomic(os.path.join(d, JOB_NAME), job)
    # Exclusive create: an id is never queued twice.
    fd = os.open(marker_path(analysis_id, 'queued'), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    os.close(fd)
    return job


def load_job(analysis_id):
    d = run_dir(analysis_id)
    return read_json(os.path.join(d, JOB_NAME)) if d else None


def write_status(analysis_id, **fields):
    """The worker's progress record; read back by status polls."""
    d = run_dir(analysis_id)
    fields.setdefault('updated_utc', utc_now_iso())
    write_json_atomic(os.path.join(d, STATUS_NAME), fields)


def read_status(analysis_id):
    d = run_dir(analysis_id)
    return read_json(os.path.join(d, STATUS_NAME), {}) if d else {}


def write_heartbeat(pid, running_ids):
    write_json_atomic(os.path.join(QUEUE_DIR, 'worker.json'), {
        'pid': pid, 'slots': SLOTS, 'running': running_ids,
        'updated_utc': utc_now_iso(), 'updated_unix': time.time(),
    })


def worker_alive():
    hb = read_json(os.path.join(QUEUE_DIR, 'worker.json'), {})
    return bool(hb) and (time.time() - float(hb.get('updated_unix', 0))) < WORKER_STALE_S


def record_duration(seconds):
    """Keep the last STATS_KEEP run durations for the estimate (worker only)."""
    p = os.path.join(QUEUE_DIR, 'stats.json')
    stats = read_json(p, {}) or {}
    durations = [float(x) for x in stats.get('durations_s', [])][-(STATS_KEEP - 1):]
    durations.append(round(float(seconds), 1))
    write_json_atomic(p, {'durations_s': durations, 'updated_utc': utc_now_iso()})


def typical_duration():
    """Median of recent run durations, or the default before any exist."""
    stats = read_json(os.path.join(QUEUE_DIR, 'stats.json'), {}) or {}
    d = sorted(float(x) for x in stats.get('durations_s', []))
    if not d:
        return DEFAULT_DURATION_S
    mid = len(d) // 2
    return d[mid] if len(d) % 2 else (d[mid - 1] + d[mid]) / 2


def queue_status(analysis_id):
    """Where an analysis stands, for the status endpoint.

    Returns None if the id is malformed or unknown. Otherwise a dict with
    `state` in queued | running | done | failed, and for the two pending
    states the position, the queue depth, how many are running, the
    worker's heartbeat, the current stage, and an estimate in seconds.
    """
    d = run_dir(analysis_id)
    if not d or not os.path.isdir(d):
        return None
    if os.path.isfile(os.path.join(d, MANIFEST_NAME)):
        return {'state': 'done'}
    failed = read_json(os.path.join(d, FAILED_NAME))
    if failed:
        return {'state': 'failed', 'message': failed.get('error') or 'Analysis failed'}
    job = read_json(os.path.join(d, JOB_NAME))
    if not job:
        return None
    queued, running = list_markers()
    alive = worker_alive()
    median = typical_duration()
    out = {
        'mode': job.get('mode'),
        'label': job.get('label', ''),
        'created_utc': job.get('created_utc'),
        'request': job.get('request'),
        'ipp': (job.get('extra') or {}).get('ipp'),
        'isochrone_params': (job.get('extra') or {}).get('isochrone_params'),
        'queued': len(queued),
        'running': len(running),
        'slots': SLOTS,
        'worker_alive': alive,
        'typical_s': round(median),
    }
    if analysis_id in running:
        st = read_status(analysis_id)
        stage = st.get('stage') or 'dem'
        idx = STAGE_INDEX.get(stage, 0)
        started = st.get('started_unix')
        elapsed = (time.time() - float(started)) if started else 0.0
        out.update({
            'state': 'running',
            'stage': stage,
            'stage_message': dict(STAGES).get(stage, 'Running...'),
            'stage_index': idx + 1,
            'stage_count': len(STAGES) - (0 if job.get('mode') == 'tarr' else 1),
            'elapsed_s': round(elapsed),
            'eta_done_s': round(max(median - elapsed, 5)),
        })
        return out
    if analysis_id in queued:
        pos = queued.index(analysis_id)         # 0-based: how many wait ahead
        # Slots free up one `median` at a time, SLOTS at a time. This job
        # starts after ceil((pos + 1) / SLOTS) such rounds when every slot
        # is busy, and at once when one is free (the worker polls every
        # second), unless the worker is down, in which case no estimate is
        # honest and the browser says so instead.
        if len(running) >= SLOTS:
            rounds = math.ceil((pos + 1) / SLOTS)
            start = median * rounds
        else:
            start = 0.0
        out.update({
            'state': 'queued',
            'position': pos + 1,                 # 1-based for display
            'eta_start_s': round(start),
            'eta_done_s': round(start + median),
        })
        return out
    # A job with no marker and no record: the worker gave up on it (see
    # worker.recover) or the marker was lost. Report it as failed rather
    # than leave a browser polling forever.
    return {'state': 'failed', 'message': 'The analysis was lost from the queue. Please run it again.'}
