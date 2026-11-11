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
from . import metrics, worker_metrics

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
                worker_metrics.REAPED.labels(action="requeued").inc(requeued)
                _log("reaper requeued %d job(s)" % requeued)
            if dead:
                worker_metrics.REAPED.labels(action="dead").inc(dead)
                _log("reaper dead-lettered %d timed-out job(s)" % dead)
        except Exception as exc:  # never let the loop die on a transient error
            worker_metrics.BACKGROUND_ERRORS.labels(loop="reaper").inc()
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
            worker_metrics.BACKGROUND_ERRORS.labels(loop="promoter").inc()
            _log("promoter error: %s" % exc)
        _stop.wait(config.PROMOTE_INTERVAL)


def _process(job_id):
    """Run a single claimed job and record the outcome.

    All labels are bounded: the job type is mapped to a registered type or
    ``unregistered``, the priority to a configured priority or ``unknown``, and
    handler failures use only the exception class name.
    """
    job = queue.get_job(job_id)
    if job is None:
        # Hash vanished (expired or manually deleted). Drop the lease so it does
        # not linger inflight; nothing left to run.
        queue.ack(job_id)
        _log("claimed %s but its record was gone, dropped" % job_id)
        return

    job_type = job["type"]
    type_label = metrics.job_type_label(job_type)
    priority = metrics.priority_label(job.get("priority"))
    handler = jobs.get_handler(job_type)

    started_wall = time.time()
    try:
        created_at = float(job.get("created_at", started_wall))
    except (TypeError, ValueError):
        created_at = started_wall
    try:
        attempts_before = int(job.get("attempts", 0))
    except (TypeError, ValueError):
        attempts_before = 0

    worker_metrics.WORKER_INFLIGHT.inc()
    worker_metrics.QUEUE_WAIT.labels(
        job_type=type_label, priority=priority
    ).observe(max(0.0, started_wall - created_at))

    started = time.perf_counter()
    outcome = "error"
    try:
        try:
            if handler is None:
                # No handler registered for this type. Treat as a failure so the
                # retry/dead-letter machinery handles it rather than losing it.
                raise RuntimeError("no handler registered for job type %r" % job_type)

            # Handlers receive the stored payload plus the job id. The id lets a
            # handler include it in output filenames without changing the
            # handler signature used by every existing job type.
            payload = dict(job["payload"])
            payload["_job_id"] = job_id
            result = handler(payload)
        except Exception as exc:
            worker_metrics.HANDLER_ERRORS.labels(
                job_type=type_label,
                error_type=metrics.error_type_label(exc),
            ).inc()
            outcome = queue.fail(job_id, exc)
            worker_metrics.JOBS_PROCESSED.labels(
                job_type=type_label, outcome=outcome, priority=priority
            ).inc()
            if outcome == "retry":
                worker_metrics.RETRY_DELAY.labels(job_type=type_label).observe(
                    queue.retry_delay_seconds(attempts_before)
                )
            _log("failed %s (%s): %s -> %s" % (job_id, job_type, exc, outcome))
        else:
            # Handler succeeded. ack returns False if our lease had already
            # expired and the reaper took the job back, in which case another
            # worker owns it now and we must not count it as our completion.
            try:
                acked = queue.ack(job_id, result)
            except Exception as exc:
                outcome = queue.fail(job_id, exc)
                worker_metrics.JOBS_PROCESSED.labels(
                    job_type=type_label, outcome=outcome, priority=priority
                ).inc()
                _log("ack failed for %s (%s): %s -> %s" % (job_id, job_type, exc, outcome))
            else:
                if acked:
                    outcome = "completed"
                    worker_metrics.JOBS_PROCESSED.labels(
                        job_type=type_label, outcome=outcome, priority=priority
                    ).inc()
                    worker_metrics.JOB_TOTAL_TIME.labels(
                        job_type=type_label, priority=priority
                    ).observe(max(0.0, time.time() - created_at))
                    _log("completed %s (%s)" % (job_id, job_type))
                else:
                    outcome = "lost"
                    worker_metrics.JOBS_PROCESSED.labels(
                        job_type=type_label, outcome=outcome, priority=priority
                    ).inc()
                    _log("completed %s but lease was lost, not acked" % job_id)
    finally:
        worker_metrics.JOB_LATENCY.labels(
            job_type=type_label, outcome=outcome
        ).observe(time.perf_counter() - started)
        worker_metrics.WORKER_INFLIGHT.dec()


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
