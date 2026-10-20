#!/usr/bin/env python3
"""The analysis queue worker: runs queued analyses, SLOTS at a time.

    venv/bin/python3 worker.py                 the service (wisar-worker.service)
    venv/bin/python3 worker.py --run <id>      one analysis, as a child of the service

The web app (server.py) no longer runs analyses itself. It validates a
request, writes job.json into a new run directory, drops a marker in
RUNS_DIR/.queue and answers at once; the browser then polls for status.
This process watches that directory, claims the oldest marker by renaming
it from .queued to .running, and starts a child process for it. When the
child exits the marker is removed. The child writes status.json as each
pipeline step completes, and manifest.json (or failed.json) at the end,
exactly as the synchronous endpoints used to.

Why a child process per analysis, not a thread: the cost-distance step is
a pure-Python loop that cannot be interrupted from another thread, and a
hung GDAL read would hang a thread for good. A child can be killed, its
memory comes back to the host at once (a long-lived gunicorn worker on
the Linode had grown to 2.8 GB from past analyses), and the queue moves
on. (The canyons worker on the same host held its queue for 67 minutes in
2026-10 for want of exactly this.)

Recovery: on startup every .running marker is a job this process was
killed under. Its directory is checked; a finished one is released, an
unfinished one is requeued, and one that has already been retried fails
with a record. A deploy therefore restarts this service freely: a job in
flight is re-run from the start, not lost.

Everything the service needs is the working directory (app/), the
WISAR_RUNS_DIR environment variable if it is not the production path, and
WISAR_WORKER_SLOTS (default 2).
"""
import os
import signal
import subprocess
import sys
import time
import traceback

import store
from store import (SLOTS, STAGES, STAGE_INDEX, MANIFEST_NAME, FAILED_NAME,
                   run_dir, marker_path, list_markers, load_job, write_json_atomic,
                   write_status, write_heartbeat, record_duration, record_failure,
                   save_result, utc_now_iso)

POLL_S = 1.0
HEARTBEAT_S = 5.0
# A job past this is killed and recorded as failed. The longest honest
# analysis (Travel Time at 3 mph x 12 h, 1000 px grid) is a few minutes;
# anything near this is a hung fetch or a runaway.
JOB_TIMEOUT_S = float(os.environ.get('WISAR_JOB_TIMEOUT', '1500'))
MAX_ATTEMPTS = 2
GRACE_S = 10.0


def log(msg):
    print(f"[worker {utc_now_iso()}] {msg}", flush=True)


# ============================================================
# Child: run one analysis
# ============================================================

def run_one(analysis_id):
    job = load_job(analysis_id)
    if not job:
        log(f"{analysis_id}: no job.json, nothing to run")
        return 3
    mode = job['mode']
    started_at = job.get('created_utc') or utc_now_iso()
    wd = run_dir(analysis_id)
    t0 = time.time()
    stage_names = [name for name, _ in STAGES]

    def report(stage):
        write_status(analysis_id, state='running', stage=stage,
                     started_utc=job.get('started_utc'), started_unix=t0,
                     pid=os.getpid(), attempt=job.get('attempts', 0))

    def on_step_done(step):
        # The pipeline marks a step when it finishes; show the next one.
        i = STAGE_INDEX.get(step)
        if i is None:
            return
        nxt = i + 1
        if mode != 'tarr' and nxt < len(stage_names) and stage_names[nxt] == 'probability':
            nxt += 1
        report(stage_names[min(nxt, len(stage_names) - 1)])

    report('dem')
    try:
        from pipeline import shared
        shared.STEP_HOOK = on_step_done
        params = dict(job['params'])
        if mode == 'tarr':
            from pipeline import run_analysis
            result = run_analysis(work_dir=wd, **params)
        else:
            from pipeline import run_isochrone_analysis
            result = run_isochrone_analysis(work_dir=wd, **params)
        result.setdefault('timings_s', {})['total'] = round(time.time() - t0, 2)
        save_result(analysis_id, result, mode, job.get('request'), job.get('label', ''),
                    started_at, job.get('extra') or {})
        log(f"{analysis_id}: saved in {result['timings_s']['total']} s")
        return 0
    except Exception as e:
        traceback.print_exc()
        record_failure(analysis_id, mode, job.get('request'), job.get('label', ''),
                       started_at, e)
        return 1


# ============================================================
# Service: the queue loop
# ============================================================

class Worker:
    def __init__(self):
        self.children = {}      # analysis_id -> (Popen, started_unix)
        self.stopping = False
        self.last_heartbeat = 0.0

    # -- markers -------------------------------------------------------
    def release(self, analysis_id):
        for state in ('running', 'queued'):
            try:
                os.remove(marker_path(analysis_id, state))
            except FileNotFoundError:
                pass

    def recover(self):
        """Settle every .running marker left by a previous process."""
        _, running = list_markers()
        for aid in running:
            d = run_dir(aid)
            job = load_job(aid)
            if not d or not os.path.isdir(d) or not job:
                log(f"recover {aid}: no run directory or job, marker dropped")
                self.release(aid)
                continue
            if os.path.isfile(os.path.join(d, MANIFEST_NAME)) or os.path.isfile(os.path.join(d, FAILED_NAME)):
                log(f"recover {aid}: already finished, marker dropped")
                self.release(aid)
                continue
            attempts = int(job.get('attempts', 0))
            if attempts >= MAX_ATTEMPTS:
                log(f"recover {aid}: interrupted {attempts} times, giving up")
                record_failure(aid, job.get('mode'), job.get('request'), job.get('label', ''),
                               job.get('created_utc'),
                               'The analysis was interrupted twice (server restart) and was not retried again.',
                               tb='')
                self.release(aid)
                continue
            log(f"recover {aid}: interrupted, requeued (attempt {attempts + 1})")
            os.replace(marker_path(aid, 'running'), marker_path(aid, 'queued'))

    # -- children ------------------------------------------------------
    def start(self, analysis_id):
        job = load_job(analysis_id)
        if not job:
            log(f"{analysis_id}: marker without job.json, dropped")
            self.release(analysis_id)
            return
        try:
            os.replace(marker_path(analysis_id, 'queued'), marker_path(analysis_id, 'running'))
        except FileNotFoundError:
            return                      # claimed by someone else, or withdrawn
        job['attempts'] = int(job.get('attempts', 0)) + 1
        job['started_utc'] = utc_now_iso()
        write_json_atomic(os.path.join(run_dir(analysis_id), store.JOB_NAME), job)
        write_status(analysis_id, state='running', stage='dem', started_utc=job['started_utc'],
                     started_unix=time.time(), attempt=job['attempts'])
        p = subprocess.Popen([sys.executable, os.path.abspath(__file__), '--run', analysis_id],
                             cwd=os.path.dirname(os.path.abspath(__file__)))
        self.children[analysis_id] = (p, time.time())
        log(f"{analysis_id}: started (pid {p.pid}, attempt {job['attempts']}, "
            f"{len(self.children)}/{SLOTS} slots busy)")

    def reap(self):
        for aid, (p, t_start) in list(self.children.items()):
            rc = p.poll()
            if rc is None:
                if time.time() - t_start > JOB_TIMEOUT_S:
                    log(f"{aid}: over {JOB_TIMEOUT_S:.0f} s, killing")
                    p.kill()
                    p.wait()
                    self.fail_if_unrecorded(aid, f'The analysis ran longer than {JOB_TIMEOUT_S / 60:.0f} minutes and was stopped.')
                    del self.children[aid]
                    self.release(aid)
                continue
            del self.children[aid]
            d = run_dir(aid)
            if rc == 0 and os.path.isfile(os.path.join(d, MANIFEST_NAME)):
                m = store.read_json(os.path.join(d, MANIFEST_NAME), {}) or {}
                total = (m.get('timings_s') or {}).get('total')
                if total:
                    record_duration(total)
                log(f"{aid}: done in {total} s")
            else:
                self.fail_if_unrecorded(aid, f'The analysis process exited with code {rc}.')
                log(f"{aid}: failed (exit {rc})")
            self.release(aid)

    def fail_if_unrecorded(self, aid, message):
        d = run_dir(aid)
        if os.path.isfile(os.path.join(d, FAILED_NAME)) or os.path.isfile(os.path.join(d, MANIFEST_NAME)):
            return
        job = load_job(aid) or {}
        record_failure(aid, job.get('mode'), job.get('request'), job.get('label', ''),
                       job.get('created_utc'), message, tb='')

    def stop(self, *_):
        self.stopping = True

    def shutdown(self):
        """Terminate children and leave their markers .running: the next
        process requeues them (see recover)."""
        if self.children:
            log(f"stopping with {len(self.children)} running; they will be requeued")
        for aid, (p, _) in self.children.items():
            if p.poll() is None:
                p.terminate()
        deadline = time.time() + GRACE_S
        for aid, (p, _) in self.children.items():
            try:
                p.wait(timeout=max(0.1, deadline - time.time()))
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()

    # -- loop ----------------------------------------------------------
    def run(self):
        # One worker per store. Two instances would both claim jobs and
        # run twice SLOTS at once (seen in the fork's testing, 2026-10-19,
        # after a manual restart beside a live one). The lock is released
        # by the kernel when this process ends, however it ends.
        import fcntl
        self._lock = open(os.path.join(store.QUEUE_DIR, 'worker.lock'), 'w')
        try:
            fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            log(f"another worker already holds {self._lock.name}; exiting")
            return
        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGINT, self.stop)
        log(f"starting: {SLOTS} slots, store {store.RUNS_DIR}, timeout {JOB_TIMEOUT_S:.0f} s")
        self.recover()
        while not self.stopping:
            self.reap()
            if not self.stopping:
                queued, _ = list_markers()
                for aid in queued:
                    if len(self.children) >= SLOTS:
                        break
                    if aid not in self.children:
                        self.start(aid)
            now = time.time()
            if now - self.last_heartbeat >= HEARTBEAT_S or self.children:
                write_heartbeat(os.getpid(), sorted(self.children))
                self.last_heartbeat = now
            time.sleep(POLL_S)
        self.shutdown()
        log("stopped")


def main(argv):
    if len(argv) >= 3 and argv[1] == '--run':
        return run_one(argv[2])
    if len(argv) > 1:
        print(__doc__)
        return 2
    Worker().run()
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
