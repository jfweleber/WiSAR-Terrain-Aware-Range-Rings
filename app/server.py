from flask import Flask, jsonify, request, send_from_directory, send_file
import urllib.request
import urllib.parse
import urllib.error
import json
import os
import threading
import traceback
import base64
import hmac as hmac_mod
import hashlib
import time

app = Flask(__name__, static_folder='static')
analyses = {}
analysis_lock = threading.Lock()

# Coconino County calibration multipliers derived from Phase 2 validation
# (n=360 subjects, 253 missions). Each multiplier is the single value that
# minimizes total absolute error across p25/p50/p75 containment rates.
# Profiles with n<20 fall back to the global multiplier (1.40).
CALIBRATION_MULTIPLIERS = {
    'Hiker':          1.15,   # n=183
    'Skier (Alpine)': 2.15,   # n=36
    'Dementia':       2.55,   # n=29
    'Mental Illness': 4.05,   # n=25
    'Despondent':     1.65,   # n=21
    'Hunter':         0.80,   # n=21
    'Child (10-12)':  1.55,   # n=20
}
CALIBRATION_DEFAULT = 1.40
RESULTS_DIR = '/tmp/wisar_results'
os.makedirs(RESULTS_DIR, exist_ok=True)

# ============================================================
# CalTopo API write credentials (CCSO-SAR service account)
# ------------------------------------------------------------
# Loaded from environment variables set by the systemd unit
# (/etc/systemd/system/wisar.service) via Environment= lines.
# Credentials never enter source code or version control.
#
# If any value is missing at startup, the server still boots,
# but the CCSO-mode CalTopo push path will fail authentication
# with CalTopo's API. A warning is logged below to make this
# diagnosable from journalctl rather than mystifying 401s.
# ============================================================
CALTOPO_ACCOUNT_ID = os.environ.get('CALTOPO_ACCOUNT_ID', '')
CALTOPO_CREDENTIAL_ID = os.environ.get('CALTOPO_CREDENTIAL_ID', '')
CALTOPO_CREDENTIAL_KEY = os.environ.get('CALTOPO_CREDENTIAL_KEY', '')
CALTOPO_BASE_URL = 'https://caltopo.com'

# Startup sanity check: warn (don't crash) if any CCSO credential
# is missing. Avoids logging the actual values — only reports
# presence/absence so the warning is safe to appear in logs.
_ccso_missing = [
    name for name, value in (
        ('CALTOPO_ACCOUNT_ID', CALTOPO_ACCOUNT_ID),
        ('CALTOPO_CREDENTIAL_ID', CALTOPO_CREDENTIAL_ID),
        ('CALTOPO_CREDENTIAL_KEY', CALTOPO_CREDENTIAL_KEY),
    ) if not value
]
if _ccso_missing:
    print(f"WARNING: CCSO CalTopo credentials missing from environment: {_ccso_missing}. "
          f"CCSO-mode CalTopo push will fail until these are set in the systemd unit.")
del _ccso_missing

def caltopo_sign(method, url_path, expires, payload_string, credential_key):
    """Generate HMAC-SHA256 signature for a CalTopo API request.

    credential_key is the base64-encoded HMAC secret for the calling team.
    Passed in as a parameter (rather than read from a global) so this
    function can serve both CCSO-default and other-team request paths
    from Phase 4 onward.
    """
    message = f"{method} {url_path}\n{expires}\n{payload_string}"
    secret = base64.b64decode(credential_key)
    signature = hmac_mod.new(secret, message.encode(), hashlib.sha256).digest()
    return base64.b64encode(signature).decode()

def caltopo_api_request(method, endpoint, payload, account_id, credential_id, credential_key):
    """Send an authenticated request to the CalTopo API.

    Credentials (account_id, credential_id, credential_key) are passed in
    as parameters so the same function serves both CCSO-default callers
    (which source credentials from module-level env-var loads) and
    other-team callers (which receive credentials from the request body,
    held only in the request handler's local scope).

    Wire format (v1.14): auth parameters (id, expires, signature) are always
    transmitted as URL query parameters. The JSON payload, when present,
    goes in the request body as form-encoded data under the 'json' key —
    NOT appended to the URL. This matches CalTopo's official reference
    implementations in their Team API documentation and lifts the practical
    URL-length limit that previously forced aggressive geometry simplification
    on large TARR / travel-time polygons (see server.py git history — the
    old URL-encoded path capped at roughly 500 vertices per feature before
    CalTopo would reject the request).

    Signature computation is unchanged: HMAC over the canonical string
    "{METHOD} {PATH}\\n{expires}\\n{payload_string}". The signature doesn't
    care where the payload is transmitted, only that it matches what
    CalTopo sees on arrival — which we control by sending the exact
    payload_string as the value of the body's 'json' key.

    For GET requests (or POST/PUT with no payload), the behavior is
    effectively unchanged — there's no body to send, so we issue a simple
    authenticated request against the URL.

    account_id is currently accepted but not used in the signed request
    itself — CalTopo identifies the team via credential_id alone. It's
    kept in the function signature for symmetry with the way the frontend
    collects credentials (three fields together) and so future endpoints
    that DO need the team ID can use the same call signature.
    """
    expires = int((time.time() + 120) * 1000)
    payload_string = json.dumps(payload) if payload else ''
    signature = caltopo_sign(method, endpoint, expires, payload_string, credential_key)

    # Auth params always go on the URL regardless of method.
    sep = '&' if '?' in endpoint else '?'
    url = (f"{CALTOPO_BASE_URL}{endpoint}{sep}"
           f"id={credential_id}"
           f"&expires={expires}"
           f"&signature={urllib.request.quote(signature, safe='')}")

    # For POST/PUT with a payload, send 'json=<payload>' as the body in
    # application/x-www-form-urlencoded format. urllib.parse.urlencode
    # handles the percent-encoding so the payload string round-trips
    # byte-for-byte to CalTopo's parser — critical for signature match.
    body_bytes = None
    headers = {'User-Agent': 'WiSAR-Decision-Support/0.1'}
    if payload_string and method.upper() in ('POST', 'PUT'):
        body_bytes = urllib.parse.urlencode({'json': payload_string}).encode('utf-8')
        headers['Content-Type'] = 'application/x-www-form-urlencoded'

    req = urllib.request.Request(url, data=body_bytes, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=30) as response:
        body = response.read().decode()
        if body.strip():
            return json.loads(body)
        else:
            return {'status': 'ok', 'result': {}}

def save_result(analysis_id, result):
    path = os.path.join(RESULTS_DIR, analysis_id + '.json')
    with open(path, 'w') as f:
        json.dump(result, f)
    with analysis_lock:
        analyses[analysis_id] = result

def load_result(analysis_id):
    with analysis_lock:
        if analysis_id in analyses:
            return analyses[analysis_id]
    path = os.path.join(RESULTS_DIR, analysis_id + '.json')
    if os.path.exists(path):
        with open(path, 'r') as f:
            result = json.load(f)
        with analysis_lock:
            analyses[analysis_id] = result
        return result
    return None

@app.route('/')
def index():
    return send_from_directory('static', 'index.html')

@app.route('/api/analyze', methods=['POST'])
def run_analysis_endpoint():
    try:
        data = request.get_json()
        if not data:
            return jsonify({'status':'error','message':'No JSON data provided'}), 400
        ipp = data.get('ipp', {})
        ipp_lat = float(ipp.get('lat', 0))
        ipp_lng = float(ipp.get('lng', 0))
        if ipp_lat == 0 or ipp_lng == 0:
            return jsonify({'status':'error','message':'Invalid IPP coordinates'}), 400
        percentiles = data.get('percentiles', {})
        p25 = float(percentiles.get('p25', 0)) if percentiles else 0
        p50 = float(percentiles.get('p50', 0)) if percentiles else 0
        p75 = float(percentiles.get('p75', 0)) if percentiles else 0
        has_percentiles = p25 > 0 and p50 > 0 and p75 > 0
        if has_percentiles and (p25 >= p50 or p50 >= p75):
            return jsonify({'status':'error','message':'Percentiles must be three increasing positive values'}), 400
        # Apply Coconino calibration multiplier to Koester percentiles
        profile_name = data.get('profile', '')
        multiplier = 1.0
        if has_percentiles and profile_name:
            multiplier = CALIBRATION_MULTIPLIERS.get(profile_name, CALIBRATION_DEFAULT)
            p25 *= multiplier
            p50 *= multiplier
            p75 *= multiplier
            print(f"  Calibration: {profile_name} x{multiplier:.2f} -> p25={p25:.2f}, p50={p50:.2f}, p75={p75:.2f} km")
        if not has_percentiles:
            p25, p50, p75 = 1.0, 2.0, 3.0  # dummy values, won't be used
        # Radius auto-computed from calibrated p75 + 2 km padding, ensuring the
        # bounding box fully contains all three TARR contours.
        radius_km = p75 + 2.0
        from pipeline import run_analysis
        result = run_analysis(ipp_lat=ipp_lat, ipp_lng=ipp_lng,
            pct_25_km=p25, pct_50_km=p50, pct_75_km=p75,
            radius_km=radius_km)
        analysis_id = f"{ipp_lat:.4f}_{ipp_lng:.4f}"
        # Store percentiles in result for PNG renderer
        result['percentiles'] = {'p25': p25, 'p50': p50, 'p75': p75}
        save_result(analysis_id, result)
        import rasterio
        with rasterio.open(result['probability_path']) as src:
            bounds = src.bounds
        contour_geojson = result.get('contour_geojson', None)
        # Data-source warnings (e.g., OSM fell back to cache, or cache was
        # unavailable entirely). Default to empty list so the frontend can
        # always iterate over it without a null check.
        warnings = result.get('warnings', [])
        return jsonify({'status':'ok','analysis_id':analysis_id,
            'has_percentiles':has_percentiles,
            'calibration': {'profile': profile_name, 'multiplier': multiplier} if multiplier != 1.0 else None,
            'contour_geojson':contour_geojson,
            'warnings':warnings,
            'bounds':{'west':bounds.left,'south':bounds.bottom,'east':bounds.right,'north':bounds.top},
            'cost_surface_url':f'/api/results/{analysis_id}/cost_surface.png',
            'percentiles_url':f'/api/results/{analysis_id}/percentiles.png',
            'cost_distance_url':f'/api/results/{analysis_id}/cost_distance.tif'})
    except Exception as e:
        traceback.print_exc()
        return jsonify({'status':'error','message':str(e)}), 500


# ============================================================
# Isochrone analysis endpoint — time-based reachability mode
#
# Unlike /api/analyze which uses Koester LPB percentiles to
# define TARR contours, this endpoint lets the SAR coordinator
# specify a flat-ground travel speed and time intervals. The
# pipeline runs identically through cost-distance, then converts
# the output to travel-time isochrones.
#
# Expected JSON payload:
#   {
#     "ipp": {"lat": 36.05, "lng": -112.14},
#     "speed": 1.61,              // km/h (flat ground)
#     "speed_unit": "kmh",        // "kmh" or "mph" — server normalizes to km/h
#     "intervals": [1, 2, 4, 8],  // hours
#     "radius": 10000             // meters (analysis extent from IPP)
#   }
# ============================================================
@app.route('/api/analyze-isochrone', methods=['POST'])
def run_isochrone_endpoint():
    try:
        data = request.get_json()
        if not data:
            return jsonify({'status': 'error', 'message': 'No JSON data provided'}), 400

        # --- Parse and validate IPP coordinates ---
        ipp = data.get('ipp', {})
        ipp_lat = float(ipp.get('lat', 0))
        ipp_lng = float(ipp.get('lng', 0))
        if ipp_lat == 0 or ipp_lng == 0:
            return jsonify({'status': 'error', 'message': 'Invalid IPP coordinates'}), 400

        # --- Parse travel speed ---
        # Accept either km/h or mph; normalize to km/h internally.
        # Coordinators in the US typically think in mph, so the frontend
        # may send mph with a unit flag. 1 mph ≈ 1.609 km/h.
        speed = float(data.get('speed', 0))
        speed_unit = data.get('speed_unit', 'kmh').lower()
        if speed_unit == 'mph':
            base_speed_kmh = speed * 1.609344
        else:
            base_speed_kmh = speed
        if base_speed_kmh <= 0 or base_speed_kmh > 20:
            return jsonify({'status': 'error',
                            'message': 'Speed must be between 0 and 20 km/h (0-12.4 mph)'}), 400

        # --- Parse time intervals ---
        intervals = data.get('intervals', [])
        if not intervals or not isinstance(intervals, list):
            return jsonify({'status': 'error',
                            'message': 'Provide at least one time interval (hours)'}), 400
        # Sanitize: convert to floats, remove non-positive values, cap at 72h
        time_intervals = sorted(set(
            float(h) for h in intervals if float(h) > 0 and float(h) <= 72
        ))
        if not time_intervals:
            return jsonify({'status': 'error',
                            'message': 'No valid time intervals (must be 0-72 hours)'}), 400

        # --- Parse analysis radius ---
        radius_km = float(data.get('radius', 10000)) / 1000

        # --- Run the isochrone pipeline ---
        from pipeline import run_isochrone_analysis
        result = run_isochrone_analysis(
            ipp_lat=ipp_lat, ipp_lng=ipp_lng,
            base_speed_kmh=base_speed_kmh,
            time_intervals_hours=time_intervals,
            radius_km=radius_km
        )

        # Store result for subsequent tile/file requests using the same
        # analysis_id pattern as the TARR endpoint
        analysis_id = f"iso_{ipp_lat:.4f}_{ipp_lng:.4f}"
        result['isochrone_params'] = {
            'base_speed_kmh': round(base_speed_kmh, 4),
            'base_speed_mph': round(base_speed_kmh / 1.609344, 2),
            'intervals': time_intervals,
        }
        save_result(analysis_id, result)

        # Read bounds from the cost-distance raster for map fitting
        import rasterio
        with rasterio.open(result['cost_distance_path']) as src:
            bounds = src.bounds

        contour_geojson = result.get('contour_geojson', None)
        warnings = result.get('warnings', [])

        return jsonify({
            'status': 'ok',
            'analysis_id': analysis_id,
            'mode': 'isochrone',
            'isochrone_params': result['isochrone_params'],
            'contour_geojson': contour_geojson,
            'warnings': warnings,
            'bounds': {
                'west': bounds.left, 'south': bounds.bottom,
                'east': bounds.right, 'north': bounds.top
            },
            # Cost surface PNG is still useful for visual inspection
            'cost_surface_url': f'/api/results/{analysis_id}/cost_surface.png',
            'cost_distance_url': f'/api/results/{analysis_id}/cost_distance.tif',
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/results/<analysis_id>/<filename>')
def serve_result(analysis_id, filename):
    result = load_result(analysis_id)
    if not result:
        return jsonify({'status':'error','message':'Analysis not found'}), 404
    file_map = {'probability.tif':result.get('probability_path'),
        'cost_distance.tif':result.get('cost_distance_path'),
        'cost_surface.tif':result.get('cost_surface_path'),'dem.tif':result.get('dem_path')}
    filepath = file_map.get(filename)
    if not filepath or not os.path.exists(filepath):
        return jsonify({'status':'error','message':'File not found'}), 404
    return send_file(filepath, mimetype='image/tiff', as_attachment=True, download_name=filename)

# ===============================================================================
# Jacobs (2015) terrain-attractor rendering
#
# The "Pure" heatmap renderer below uses Matt Jacobs's 2015 PDEN findings to
# color each pixel of the search area by the strongest applicable empirical
# terrain attractor — independent of cost-distance position from the IPP.
#
# This replaced the percentile-band heatmap in v1.15 after field-user review
# (CCSO SAR coordinator feedback) preferred the Jacobs-driven framing as the
# default visualization. The TARR contours still mark cost-distance percentile
# bands on top of the heatmap; only the within-band color is Jacobs-driven.
#
# Background and the architectural reasoning that led to this approach are
# captured in /mnt/project/jacobs_heatmap_design_notes.md and the v0.2 beta
# status document.
# ===============================================================================

# Per-pixel attractor weights derived from Jacobs (2015) Table 1 PDEN values,
# normalized so the strongest finding (stream-trail intersection at ~10x) maps
# to 1.00. Each pixel's score is the MAX across applicable masks — not sum —
# because Jacobs's findings are independent empirical observations of distinct
# cell categories, not additive multipliers. See _compute_attractor_score_max.
JACOBS_WEIGHTS = {
    'stream':       0.28,   # ~2.75x PDEN
    'intersection': 1.00,   # ~10x PDEN (strongest finding)
    'low_elev':     0.35,   # ~3.5x PDEN
    'high_elev':    0.18,   # ~1.75x PDEN
    'trail':        0.55,   # ~5-7x PDEN
}


def _load_jacobs_masks(result, target_shape):
    """Load the 5-band Jacobs masks raster and return per-mask boolean arrays.

    Returns a dict keyed by 'stream', 'intersection', 'low_elev', 'high_elev',
    'trail' (matching JACOBS_WEIGHTS keys). Each value is a boolean numpy
    array of target_shape. Returns None if the masks file is missing or
    unreadable — the renderer then degrades gracefully to a cold surface.

    Backwards compatibility: if the masks file has only 4 bands (older
    analysis runs before the trail band was added), the trail mask is
    returned as all-False so the render path doesn't KeyError.
    """
    masks_path = result.get('jacobs_masks_path')
    if not masks_path or not os.path.exists(masks_path):
        return None
    import rasterio
    import numpy as np
    with rasterio.open(masks_path) as src:
        if (src.height, src.width) != target_shape:
            print(f"  Jacobs masks shape mismatch: masks={src.height}x{src.width} "
                  f"vs target={target_shape[0]}x{target_shape[1]}")
            return None
        band_count = src.count
        stack = src.read()
    masks = {
        'stream':       stack[0].astype(bool),
        'intersection': stack[1].astype(bool),
        'low_elev':     stack[2].astype(bool),
        'high_elev':    stack[3].astype(bool),
    }
    if band_count >= 5:
        masks['trail'] = stack[4].astype(bool)
    else:
        masks['trail'] = np.zeros(target_shape, dtype=bool)
    return masks


def _apply_colormap(norm):
    """Convert a [0, 1] priority array to RGB via the 11-stop ramp.

    Returns (r_arr, g_arr, b_arr) — each a float array matching norm's shape.
    Ramp: blue (cold) -> teal -> green -> yellow -> orange -> red (hot).
    """
    import numpy as np
    stops = [(0.0,  30, 80,180),  (0.08, 43,108,196), (0.17, 46,140,160),
             (0.25, 46,165,120),  (0.33, 60,185, 80), (0.42, 130,205,50),
             (0.52, 220,175, 35), (0.63, 240,150, 30), (0.75, 240,120,30),
             (0.87, 232, 70, 38), (1.0,  210, 45, 35)]
    r_arr = np.full_like(norm, 30.0)
    g_arr = np.full_like(norm, 80.0)
    b_arr = np.full_like(norm, 180.0)
    for i in range(len(stops)-1):
        t0, r0, g0, b0 = stops[i]
        t1, r1, g1, b1 = stops[i+1]
        mask = (norm >= t0) & (norm < t1) if i < len(stops)-2 else (norm >= t0) & (norm <= t1)
        frac = np.where(mask, (norm - t0) / (t1 - t0), 0)
        r_arr = np.where(mask, r0 + frac * (r1 - r0), r_arr)
        g_arr = np.where(mask, g0 + frac * (g1 - g0), g_arr)
        b_arr = np.where(mask, b0 + frac * (b1 - b0), b_arr)
    return r_arr, g_arr, b_arr


def _compute_attractor_score_max(jacobs, nodata_mask, shape):
    """Per-pixel Jacobs attractor score using max(), not sum.

    Each cell reads at the value of its strongest applicable attractor.
    This matches the structure of Jacobs's (2015) findings — his PDEN
    figures for "stream proximity," "trail proximity," "stream-trail
    intersection," etc. are separate empirical measurements of distinct
    cell categories, NOT additive lifts. A stream-trail intersection cell
    is ALSO a stream cell and ALSO a trail cell in our masks, but Jacobs's
    10x intersection PDEN already accounts for that overlap — summing
    stream + trail + intersection weights would double-count.

    Returns a float array in [0, 1] of the given shape. Zero where no
    masks apply; otherwise the maximum weight among applicable masks.
    Zero where nodata_mask is True.

    Returns all-zeros if jacobs is None (masks file missing — graceful
    degradation rather than 500 error).
    """
    import numpy as np
    score = np.zeros(shape, dtype=np.float32)
    if jacobs is None:
        return score
    for mask_key, weight in JACOBS_WEIGHTS.items():
        mask = jacobs[mask_key] & (~nodata_mask)
        if np.any(mask):
            score = np.where(mask, np.maximum(score, weight), score)
    return score


@app.route('/api/results/<analysis_id>/cost_surface.png')
def serve_cost_png(analysis_id):
    """Render the analysis heatmap (Jacobs Pure formulation as of v1.15).

    Per-pixel color is driven entirely by the strongest applicable Jacobs
    (2015) terrain-attractor signal — cost-distance position from the IPP
    contributes nothing to within-band color. The TARR contour polygons
    (drawn separately by the frontend) still mark the p25/p50/p75 envelope,
    so coordinators see both: the cost-distance envelope as contour lines,
    and the within-envelope priority as Jacobs-driven color.

    Renders across the FULL cost-distance raster — no alpha fade past p75,
    no attractor zeroing past p75. The "1-in-4 finds occur outside p75"
    reality plus Jacobs's empirical finding that linear-feature PDEN
    INCREASES with IPP-find distance both argue for keeping attractor
    signal visible to the bbox edge.

    Endpoint URL kept as `cost_surface.png` for backward compatibility with
    the frontend's existing image-overlay wiring. The name is now slightly
    misleading (this isn't the cost surface anymore) but renaming would
    cascade into more frontend changes for no behavioral gain.
    """
    result = load_result(analysis_id)
    if not result:
        return jsonify({'status':'error','message':'Analysis not found'}), 404
    cd_path = result.get('cost_distance_path')
    if not cd_path or not os.path.exists(cd_path):
        return jsonify({'status':'error','message':'File not found'}), 404
    try:
        import rasterio
        import numpy as np
        from PIL import Image
        import io
        with rasterio.open(cd_path) as src:
            data = src.read(1).astype(np.float64)
        nodata_mask = (data <= 0) | (data == -9999) | np.isinf(data) | np.isnan(data)
        height, width = data.shape

        # Per-pixel Jacobs attractor score (max of applicable weights).
        # This is the entire color signal — no cost-distance contribution.
        # Attractor scores are computed across the full raster — no clipping
        # at p75. Coordinators see Jacobs-strong features (intersections,
        # trails, low pockets) wherever they appear in the search bbox.
        jacobs = _load_jacobs_masks(result, (height, width))
        attractor_score = _compute_attractor_score_max(jacobs, nodata_mask, (height, width))
        attractor_score[nodata_mask] = 0.0

        # Render through the shared colormap
        r_arr, g_arr, b_arr = _apply_colormap(attractor_score)
        rgba = np.zeros((height, width, 4), dtype=np.uint8)
        rgba[:,:,0] = r_arr.clip(0,255).astype(np.uint8)
        rgba[:,:,1] = g_arr.clip(0,255).astype(np.uint8)
        rgba[:,:,2] = b_arr.clip(0,255).astype(np.uint8)

        # Variable alpha tied to attractor strength via a gamma-curve ramp.
        # Cold cells (no Jacobs signal, score=0) stay at ALPHA_FLOOR; mid-range
        # categories (stream, low-elev, trail) get a disproportionate boost
        # because gamma<1 lifts mid-range scores more than the endpoints. This
        # matches field-testing feedback that the green stream signal along
        # the Colorado was too faint to register without making the cold blue
        # zones any louder.
        #
        # Formula: alpha = ALPHA_FLOOR + ALPHA_RANGE * (attractor_score ** ALPHA_GAMMA)
        #
        # Resulting alpha / effective opacity (with 0.6 overlay multiplier):
        #   score 0.00 (no attractor)    -> alpha 60  (~14%) — unchanged from prior
        #   score 0.18 (high_elev)       -> alpha 120 (~28%)
        #   score 0.28 (stream)          -> alpha 139 (~33%)
        #   score 0.35 (low_elev)        -> alpha 150 (~35%)
        #   score 0.55 (trail)           -> alpha 178 (~42%)
        #   score 1.00 (intersection)    -> alpha 230 (~54%)
        #
        # All three parameters are tunable. Lower gamma compresses the curve
        # toward the floor (more uniform visibility); raise it back to 1.0 for
        # a linear ramp. Adjust ALPHA_RANGE to shift the hot end.
        ALPHA_FLOOR = 60
        ALPHA_RANGE = 170
        ALPHA_GAMMA = 0.6
        alpha = np.where(
            nodata_mask,
            0,
            ALPHA_FLOOR + ALPHA_RANGE * (attractor_score ** ALPHA_GAMMA)
        ).clip(0, 255).astype(np.uint8)
        rgba[:,:,3] = alpha

        img = Image.fromarray(rgba, 'RGBA')
        buf = io.BytesIO()
        img.save(buf, format='PNG')
        buf.seek(0)
        return send_file(buf, mimetype='image/png')
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'status':'error','message':str(e)}), 500

@app.route('/api/results/<analysis_id>/terrain.png')
def serve_terrain_png(analysis_id):
    result = load_result(analysis_id)
    if not result:
        return jsonify({'status':'error','message':'Analysis not found'}), 404
    nlcd_path = result.get('nlcd_path')
    cost_path = result.get('cost_surface_path')
    dem_path = result.get('dem_path')
    if not cost_path or not os.path.exists(cost_path):
        return jsonify({'status':'error','message':'File not found'}), 404
    try:
        import rasterio
        import numpy as np
        from PIL import Image
        import io
        from scipy.signal import convolve2d
        import math
        # Read DEM and compute slope for terrain difficulty
        with rasterio.open(cost_path) as src:
            friction = src.read(1).astype(np.float64)
            transform = src.transform
            height, width = friction.shape
        with rasterio.open(dem_path) as src:
            dem = src.read(1).astype(np.float64)
            if dem.shape != (height, width):
                from rasterio.warp import reproject, Resampling
                dem2 = np.zeros((height, width), dtype=np.float64)
                reproject(source=rasterio.band(src, 1), destination=dem2,
                    src_transform=src.transform, src_crs=src.crs,
                    dst_transform=transform, dst_crs=src.crs, resampling=Resampling.bilinear)
                dem = dem2
        dem[dem < -1000] = np.nan
        dem[dem > 10000] = np.nan
        center_lat = (transform[5] + transform[5] + transform[4] * height) / 2
        cx = abs(transform[0]) * 111320 * math.cos(math.radians(center_lat))
        cy = abs(transform[4]) * 110540
        kx = np.array([[-1,0,1],[-2,0,2],[-1,0,1]]) / (8.0 * cx)
        ky = np.array([[-1,-2,-1],[0,0,0],[1,2,1]]) / (8.0 * cy)
        dzdx = convolve2d(dem, kx, mode='same', boundary='symm')
        dzdy = convolve2d(dem, ky, mode='same', boundary='symm')
        slope_deg = np.degrees(np.arctan(np.sqrt(dzdx**2 + dzdy**2)))
        # Combine slope and friction into difficulty score 0-100
        # Slope component: 0 deg=0, 15 deg=30, 30 deg=60, 45 deg=90
        slope_score = np.clip(slope_deg * 2.0, 0, 90)
        # Friction component: 1.0=0, 1.5=25, 3.0=50, 50=95
        fric_score = np.clip((friction - 1.0) * 20.0, 0, 95)
        # Combined: max of both (terrain is as hard as its hardest component)
        difficulty = np.maximum(slope_score, fric_score)
        nodata_mask = np.isnan(dem) | (friction <= 0) | (friction == -9999)
        # Normalize 0-100 to 0-1
        norm = difficulty / 100.0
        norm = np.clip(norm, 0, 1)
        rgba = np.zeros((height, width, 4), dtype=np.uint8)
        stops = [(0.0, 20,140,40), (0.1, 50,175,50), (0.2, 100,200,45),
                 (0.3, 160,210,30), (0.4, 210,215,15), (0.5, 240,195,0),
                 (0.6, 245,150,10), (0.7, 235,100,15), (0.8, 215,55,12),
                 (0.9, 185,25,10), (1.0, 140,12,10)]
        r_arr = np.full_like(norm, 140.0)
        g_arr = np.full_like(norm, 12.0)
        b_arr = np.full_like(norm, 10.0)
        for i in range(len(stops)-1):
            t0, r0, g0, b0 = stops[i]
            t1, r1, g1, b1 = stops[i+1]
            mask = (norm >= t0) & (norm < t1) if i < len(stops)-2 else (norm >= t0) & (norm <= t1)
            frac = np.where(mask, (norm - t0) / (t1 - t0), 0)
            r_arr = np.where(mask, r0 + frac * (r1 - r0), r_arr)
            g_arr = np.where(mask, g0 + frac * (g1 - g0), g_arr)
            b_arr = np.where(mask, b0 + frac * (b1 - b0), b_arr)
        rgba[:,:,0] = r_arr.clip(0,255).astype(np.uint8)
        rgba[:,:,1] = g_arr.clip(0,255).astype(np.uint8)
        rgba[:,:,2] = b_arr.clip(0,255).astype(np.uint8)
        rgba[:,:,3] = np.where(nodata_mask, 0, 150).astype(np.uint8)
        img = Image.fromarray(rgba, 'RGBA')
        buf = io.BytesIO()
        img.save(buf, format='PNG')
        buf.seek(0)
        return send_file(buf, mimetype='image/png')
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'status':'error','message':str(e)}), 500

@app.route('/api/results/<analysis_id>/percentiles.png')
def serve_percentile_png(analysis_id):
    result = load_result(analysis_id)
    if not result:
        return jsonify({'status':'error','message':'Analysis not found'}), 404
    prob_path = result.get('probability_path')
    if not prob_path or not os.path.exists(prob_path):
        return jsonify({'status':'error','message':'File not found'}), 404
    try:
        import rasterio
        import numpy as np
        from PIL import Image, ImageDraw
        import io
        with rasterio.open(prob_path) as src:
            data = src.read(1)
        height, width = data.shape
        rgba = np.zeros((height, width, 4), dtype=np.uint8)
        # Semi-transparent filled zones
        rgba[data == 4] = [220, 38, 38, 100]
        rgba[data == 3] = [245, 158, 11, 80]
        rgba[data == 2] = [250, 204, 21, 60]
        # Draw contour lines at zone boundaries
        for zone_val, color in [(4, [255,255,255,220]), (3, [255,200,50,200]), (2, [255,100,30,200])]:
            mask = (data >= zone_val).astype(np.uint8)
            # Edge detection: find boundary pixels
            kernel_h = np.abs(np.diff(mask, axis=1))
            kernel_v = np.abs(np.diff(mask, axis=0))
            edge = np.zeros_like(mask)
            edge[:, :-1] |= kernel_h
            edge[:, 1:] |= kernel_h
            edge[:-1, :] |= kernel_v
            edge[1:, :] |= kernel_v
            # Thicken the line (2px)
            # Smooth contour lines (1px dilation for anti-aliasing)
            from scipy.ndimage import binary_dilation
            struct = np.array([[0,1,0],[1,1,1],[0,1,0]], dtype=bool)
            edge = binary_dilation(edge, structure=struct, iterations=1).astype(np.uint8)
            edge_mask = edge == 1
            rgba[edge_mask, 0] = color[0]
            rgba[edge_mask, 1] = color[1]
            rgba[edge_mask, 2] = color[2]
            rgba[edge_mask, 3] = color[3]
        img = Image.fromarray(rgba, 'RGBA')
        from PIL import ImageDraw, ImageFont
        draw = ImageDraw.Draw(img)
        try:
            font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 12)
        except:
            font = ImageFont.load_default()
        label_offsets = {4: -20, 3: 0, 2: 20}
        for zone_val, label, lcolor in [(4, '25%', (255,255,255)), (3, '50%', (255,220,80)), (2, '75%', (255,120,50))]:
            zmask = (data >= zone_val).astype(np.uint8)
            kh2 = np.abs(np.diff(zmask, axis=1))
            epts = np.zeros((height, width), dtype=np.uint8)
            epts[:, :-1] |= kh2
            target_row = height // 2 + label_offsets[zone_val]
            row_start = max(0, target_row - 5)
            row_end = min(height, target_row + 5)
            row_edge = epts[row_start:row_end, :]
            ys, xs = np.where(row_edge > 0)
            if len(xs) > 0:
                lx = int(np.max(xs)) + 5
                ly = target_row - 6
            else:
                continue
            lx = min(lx, width - 30)
            ly = max(ly, 2)
            draw.rounded_rectangle([lx-2, ly-1, lx+26, ly+13], radius=2, fill=(0,0,0,160))
            draw.text((lx, ly), label, fill=lcolor+(255,), font=font)
        buf = io.BytesIO()
        img.save(buf, format='PNG')
        buf.seek(0)
        return send_file(buf, mimetype='image/png')
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'status':'error','message':str(e)}), 500

@app.route('/api/caltopo/export-tarrs', methods=['POST'])
def export_tarrs_to_caltopo():
    """Push contour polygons to a CalTopo map as named Shape features.

    Handles both TARR contours (percentile-based) and travel-time
    isochrones (hours-based) from the same endpoint. The feature type is
    detected per-feature by checking whether the GeoJSON properties
    include an 'hours' field (present only on isochrones) — this matches
    the same detection pattern used by the frontend for KML/GeoJSON
    downloads, so a CalTopo export carries the same semantic labeling
    the coordinator sees in the locally-downloaded files.

    The endpoint URL still says 'export-tarrs' for backward compatibility
    with any existing frontend or bookmarklet callers; a future release
    can rename to 'export-contours' alongside the corresponding app.js
    update. The handler logic is mode-agnostic.
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({'status': 'error', 'message': 'No JSON data provided'}), 400
        map_id = data.get('map_id', '').strip()
        contours = data.get('contours', {})
        if not map_id:
            return jsonify({'status': 'error', 'message': 'No CalTopo map ID provided'}), 400
        features = contours.get('features', [])
        if not features:
            return jsonify({'status': 'error', 'message': 'No contour features to export'}), 400

        # ----- Team routing -----
        # 'team' field selects whose CalTopo credentials to use for this push:
        #   'ccso' (default) — use server-side env-var credentials
        #   'other'          — use credentials supplied in this request body
        # If 'team' is absent, default to 'ccso' for backward compatibility
        # with any caller that predates the multi-team feature.
        # Credentials for 'other' mode are NEVER persisted — they live only
        # in this function's local scope for the duration of the export call.
        team = (data.get('team') or 'ccso').lower().strip()
        if team == 'ccso':
            use_account_id = CALTOPO_ACCOUNT_ID
            use_credential_id = CALTOPO_CREDENTIAL_ID
            use_credential_key = CALTOPO_CREDENTIAL_KEY
            if not (use_account_id and use_credential_id and use_credential_key):
                return jsonify({
                    'status': 'error',
                    'message': 'CCSO CalTopo credentials are not configured on the server. '
                               'Contact the tool maintainer.'
                }), 500
        elif team == 'other':
            use_account_id = (data.get('account_id') or '').strip()
            use_credential_id = (data.get('credential_id') or '').strip()
            use_credential_key = (data.get('credential_key') or '').strip()
            if not (use_account_id and use_credential_id and use_credential_key):
                return jsonify({
                    'status': 'error',
                    'message': 'Account ID, Credential ID, and Credential Key are all required for Other Team mode.'
                }), 400
        else:
            return jsonify({
                'status': 'error',
                'message': f"Unknown team selector '{team}'. Expected 'ccso' or 'other'."
            }), 400

        # Detect mode from the first feature. All features in a single export
        # come from one analysis run and share the same mode. We use the
        # presence of the 'hours' property as the discriminator because
        # that's the field run_isochrone_analysis uniquely sets and the
        # frontend uses the same check elsewhere (KML/GeoJSON download).
        first_props = features[0].get('properties', {}) if features else {}
        is_isochrone = 'hours' in first_props

        # Human-readable noun for log messages and the response summary —
        # 'TARR' for percentile contours, 'travel-time contour' for isochrones.
        export_kind = 'travel-time contour' if is_isochrone else 'TARR'

        results = []
        for feature in features:
            props = feature.get('properties', {})
            geom = feature.get('geometry', {})
            color = props.get('color', '#ffffff')

            # Per-feature display label used in the CalTopo title, log lines,
            # and the results array. Different source fields depending on
            # what kind of contour this is.
            if is_isochrone:
                # Isochrones: prefer the pre-formatted 'label' (e.g. "4h")
                # set by the pipeline; fall back to constructing it from
                # 'hours' if label is missing for any reason.
                hours = props.get('hours')
                feature_label = props.get('label') or (f'{hours}h' if hours is not None else 'unknown')
                caltopo_title = f'Travel Time: {feature_label}'
                # Description contextualizes the contour for a SAR user who
                # sees it in CalTopo without the WiSAR tool open. We don't
                # include the coordinator-specified speed here because that
                # parameter is per-analysis, not per-feature, and isn't on
                # the GeoJSON payload — it's only on the pipeline's result dict.
                caltopo_description = (
                    f'{feature_label} travel-time isochrone\n'
                    f'Area reachable within {feature_label} at the coordinator-specified flat-ground speed. '
                    f'Actual travel time adjusts for terrain (Tobler slope function), land cover (NLCD), '
                    f'and available trail/road corridors (OSM).\n'
                    f'Generated by WiSAR Decision Support Tool'
                )
            else:
                # TARRs: use the existing percentile-based labeling. Keep the
                # description format identical to v1.12 — field reports and
                # screenshots in the wild reference this wording.
                feature_label = props.get('percentile', '')
                threshold_km = (props.get('threshold_m', 0) / 1000)
                caltopo_title = f'TARR {feature_label}'
                caltopo_description = (
                    f'{feature_label} Terrain-Aware Range Ring\n'
                    f'Cost-distance threshold: {threshold_km:.2f} km\n'
                    f'Generated by WiSAR Decision Support Tool'
                )

            # No geometry simplification (v1.14): body-based POST to CalTopo
            # has no practical size limit, so we can send the full-fidelity
            # polygon directly. The previous URL-encoded path forced aggressive
            # simplification to fit under CalTopo's ~16KB URL cap, which erased
            # detail on large travel-time isochrones and TARRs.
            #
            # We still round coordinates to 5 decimal places (~1.1m) because
            # that's well below both SAR display precision and the underlying
            # 30m pipeline grid resolution, and it cuts the JSON payload by
            # roughly 40% with no visible accuracy loss.
            try:
                def _round_coords(c):
                    if isinstance(c[0], (list, tuple)):
                        return [_round_coords(x) for x in c]
                    return [round(v, 5) for v in c]
                if geom.get('coordinates') is not None:
                    geom = dict(geom)
                    geom['coordinates'] = _round_coords(geom['coordinates'])
            except Exception as e:
                print(f"  Coordinate rounding warning for {feature_label}: {e}")

            shape_payload = {
                'type': 'Feature',
                'properties': {
                    'class': 'Shape',
                    'title': caltopo_title,
                    'description': caltopo_description,
                    'stroke': color, 'stroke-width': 3, 'stroke-opacity': 0.9,
                    'fill': color, 'fill-opacity': 0.08,
                },
                'geometry': geom
            }
            try:
                resp = caltopo_api_request(
                    'POST', f'/api/v1/map/{map_id}/Shape', shape_payload,
                    account_id=use_account_id,
                    credential_id=use_credential_id,
                    credential_key=use_credential_key,
                )
                # 'label' key covers both TARR percentiles and isochrone hours;
                # frontend only reads .status for success counting, so the
                # change from 'percentile' is safe.
                results.append({'label': feature_label, 'status': 'ok',
                                'id': resp.get('result', {}).get('id', 'unknown')})
                print(f"  Exported {export_kind} {feature_label} to CalTopo map {map_id}")
            except urllib.error.HTTPError as http_err:
                # CalTopo signals bad credentials with 401/403. Surface those
                # distinctly so the user knows to check their credential
                # fields. Never echo the credentials back in the response.
                if http_err.code in (401, 403):
                    print(f"  CalTopo rejected credentials (HTTP {http_err.code}) for "
                          f"{export_kind} {feature_label}; aborting remaining exports.")
                    return jsonify({
                        'status': 'auth_error',
                        'message': 'CalTopo rejected the credentials.',
                        'results': results,
                    })
                # Other HTTP errors (404 on map_id, 500 from CalTopo, etc.)
                # fall through to the generic per-feature error path.
                results.append({'label': feature_label, 'status': 'error',
                                'message': f'HTTP {http_err.code}'})
                print(f"  Failed to export {export_kind} {feature_label}: HTTP {http_err.code}")
            except Exception as e:
                # Important: str(e) here will not contain credentials because
                # caltopo_api_request never logs them and any exception from
                # urllib won't embed the URL's query string in __str__.
                results.append({'label': feature_label, 'status': 'error', 'message': str(e)})
                print(f"  Failed to export {export_kind} {feature_label}: {e}")
        ok_count = sum(1 for r in results if r['status'] == 'ok')
        # Message reflects what was actually exported — mentions the kind so
        # the UI's success notice reads correctly for either mode.
        noun_plural = 'travel-time contours' if is_isochrone else 'TARRs'
        return jsonify({'status': 'ok',
                        'message': f'Exported {ok_count}/{len(features)} {noun_plural} to CalTopo',
                        'results': results})
    except Exception as e:
        traceback.print_exc()
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
