#!/usr/bin/env python3
"""Prune rasters from old WiSAR analyses; keep every manifest and contour set.

Each analysis lives in its own directory under RUNS_DIR (see store.py):
GeoTIFFs, contours.geojson and manifest.json. After --days (default 180)
the GeoTIFFs are deleted. manifest.json, contours.geojson and failed.json
are never deleted: they are the record of what a planner was shown, and a
year of them is a few tens of megabytes. Directories that never received a
manifest or a failure record (a job lost from the queue) are removed once
they are a day old. The store's own `.queue/` directory is skipped.

    python3 prune_runs.py                 # prune
    python3 prune_runs.py --dry-run       # report only
    python3 prune_runs.py --days 365

RUNS_DIR is hardcoded here as well as in store.py, like the cache
builders, so cron does not need the package on sys.path. Change both.
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

RUNS_DIR = '/var/www/sar.weleber.net/runs'
# job.json (the request as queued) and status.json (the worker's last
# progress note) are a few hundred bytes and complete the record.
KEEP = {'manifest.json', 'contours.geojson', 'failed.json', 'job.json', 'status.json'}


def created_at(d):
    """(unix timestamp, has_record) for a run directory."""
    for name in ('manifest.json', 'failed.json'):
        p = os.path.join(d, name)
        if not os.path.isfile(p):
            continue
        try:
            with open(p) as f:
                stamp = json.load(f).get('created_utc')
            if stamp:
                dt = datetime.strptime(stamp, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
                return dt.timestamp(), True
        except Exception:
            pass
        return os.path.getmtime(p), True
    return os.path.getmtime(d), False


def dir_size(d, names):
    return sum(os.path.getsize(os.path.join(d, f)) for f in names
               if os.path.isfile(os.path.join(d, f)))


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--root', default=RUNS_DIR, help=f'analysis store (default {RUNS_DIR})')
    ap.add_argument('--days', type=float, default=180, help='keep rasters this many days (default 180)')
    ap.add_argument('--dry-run', action='store_true', help='report what would be removed')
    args = ap.parse_args()

    if not os.path.isdir(args.root):
        print(f"no such directory: {args.root}")
        return 1

    now = time.time()
    cutoff = now - args.days * 86400
    scanned = pruned = abandoned = 0
    freed = 0
    for name in sorted(os.listdir(args.root)):
        d = os.path.join(args.root, name)
        # .queue/ holds the worker's markers, heartbeat and stats, and is
        # not a run: without this it would be "abandoned" after a day.
        if not os.path.isdir(d) or name.startswith('.'):
            continue
        scanned += 1
        ts, has_record = created_at(d)
        entries = os.listdir(d)
        if not has_record:
            if ts < now - 86400:
                size = dir_size(d, entries)
                print(f"remove  {name}  (no manifest, {size / 1e6:.1f} MB)")
                if not args.dry_run:
                    for f in entries:
                        os.remove(os.path.join(d, f))
                    os.rmdir(d)
                abandoned += 1
                freed += size
            continue
        if ts >= cutoff:
            continue
        victims = [f for f in entries if f not in KEEP]
        if not victims:
            continue
        size = dir_size(d, victims)
        print(f"prune   {name}  ({len(victims)} files, {size / 1e6:.1f} MB)")
        if not args.dry_run:
            for f in victims:
                os.remove(os.path.join(d, f))
        pruned += 1
        freed += size

    verb = 'would free' if args.dry_run else 'freed'
    print(f"{scanned} runs scanned, {pruned} pruned, {abandoned} abandoned directories removed, "
          f"{verb} {freed / 1e6:.1f} MB")
    return 0


if __name__ == '__main__':
    sys.exit(main())
