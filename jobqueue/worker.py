"""Worker process: claim a job, run it, acknowledge or fail it.

Concurrency model: one job at a time per process. You scale throughput by
running more worker containers (docker compose up --scale worker=N), not by
threading inside a worker. This keeps the claim/ack story simple to reason
about and makes the atomic-claim guarantee easy to demonstrate.

Each worker also runs two lightweight background threads:
  reaper   requeues jobs whose visibility lease expired (crashed workers).
  promoter moves scheduled and backed-off jobs into the ready queues when due.

Running these in every worker means there is no single point of failure for
recovery, and it is safe to do so: the reap and promote scripts are atomic, so
many workers can run them at once without ever acting on the same job twice.
"""

import os
import signal
import socket
import threading
import time

from prometheus_client import start_http_server

from . import config, jobs, queue
from . import metrics

# Set when a shutdown signal arrives; every loop checks it to exit cleanly.
_stop = threading.Event()

WORKER_ID = "%s:%d" % (socket.gethostname(), os.getpid())


def _log(msg):
    print("[worker %s] %s" % (WORKER_ID, msg), flush=True)


def _reaper_loop():
    """Periodically requeue timed-out jobs."""
    while not _stop.is_set():
        try:
            requeued, dead = queue.reap()
            if requeued:
                metrics.REAPED.labels(action="requeued").inc(requeued)
                _log("reaper requeued %d job(s)" % requeued)
            if dead:
                metrics.REAPED.labels(action="dead").inc(dead)
                _log("reaper dead-lettered %d timed-out job(s)" % dead)
        except Exception as exc:  # never let the loop die on a transient error
            _log("reaper error: %s" % exc)
        _stop.wait(config.REAP_INTERVAL)


def _promoter_loop():
    """Periodically move due scheduled/backoff jobs into the ready queues."""
    while not _stop.is_set():
        try:
            promoted = queue.promote()
            if promoted:
                _log("promoter moved %d job(s) to ready" % promoted)
        except Exception as exc:
            _log("promoter error: %s" % exc)
        _stop.wait(config.PROMOTE_INTERVAL)


def _process(job_id):
    """Run a single claimed job and record the outcome."""
    job = queue.get_job(job_id)
    if job is None:
        # Hash vanished (expired or manually deleted). Drop the lease so it does
        # not linger inflight; nothing left to run.
        queue.ack(job_id)
        _log("claimed %s but its record was gone, dropped" % job_id)
        return

    job_type = job["type"]
    handler = jobs.get_handler(job_type)

    metrics.WORKER_INFLIGHT.inc()
    started = time.time()
    try:
        if handler is None:
            # No handler registered for this type. Treat as a failure so the
            # retry/dead-letter machinery handles it rather than losing the job.
            raise RuntimeError("no handler registered for job type %r" % job_type)

        handler(job["payload"])
        # Handler succeeded. ack returns False if our lease had already expired
        # and the reaper took the job back, in which case another worker owns it
        # now and we must not count it as our completion.
        if queue.ack(job_id):
            metrics.JOBS_PROCESSED.labels(job_type=job_type, outcome="completed").inc()
            _log("completed %s (%s)" % (job_id, job_type))
        else:
            metrics.JOBS_PROCESSED.labels(job_type=job_type, outcome="lost").inc()
            _log("completed %s but lease was lost, not acked" % job_id)
    except Exception as exc:
        outcome = queue.fail(job_id, exc)
        metrics.JOBS_PROCESSED.labels(job_type=job_type, outcome=outcome).inc()
        _log("failed %s (%s): %s -> %s" % (job_id, job_type, exc, outcome))
    finally:
        metrics.JOB_LATENCY.labels(job_type=job_type).observe(time.time() - started)
        metrics.WORKER_INFLIGHT.dec()


def _handle_signal(signum, frame):
    _log("received signal %d, shutting down after current job" % signum)
    _stop.set()


def run():
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    # Expose this worker's metrics for Prometheus to scrape.
    start_http_server(config.WORKER_METRICS_PORT)
    _log("metrics on :%d, handlers: %s" % (config.WORKER_METRICS_PORT, jobs.registered_types()))

    # Background recovery threads.
    threading.Thread(target=_reaper_loop, daemon=True, name="reaper").start()
    threading.Thread(target=_promoter_loop, daemon=True, name="promoter").start()

    _log("started, polling for jobs")
    while not _stop.is_set():
        job_id = queue.claim()
        if job_id is None:
            # Nothing ready. Sleep briefly so an empty queue does not spin the
            # CPU. _stop.wait wakes immediately on shutdown.
            _stop.wait(config.WORKER_IDLE_SLEEP)
            continue
        _process(job_id)

    _log("stopped")


if __name__ == "__main__":
    run()
