"""Prometheus metrics exported by worker processes only.

These series are process-local. Prometheus scrapes every worker replica and the
dashboard sums or rates across them. Keeping this module separate from
``jobqueue.metrics`` means the API process never registers worker gauges such as
``jobqueue_worker_inflight`` for itself.
"""

from prometheus_client import Counter, Gauge, Histogram

# Concluded delivery attempts. outcome is one of completed, retry, dead, or
# lost. A job that is retried appears here as "retry" and later as another
# outcome when the retry is delivered, so this is not a final-job counter.
JOBS_PROCESSED = Counter(
    "jobqueue_jobs_processed_total",
    "Concluded job delivery attempts, by type, outcome, and priority.",
    ["job_type", "outcome", "priority"],
)

# Handler execution time for every delivery attempt, including failures.
JOB_LATENCY = Histogram(
    "jobqueue_job_processing_seconds",
    "Handler execution time in seconds, by job type and outcome.",
    ["job_type", "outcome"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300),
)

# Enqueue-to-start wall time. For delayed jobs and retries this includes the
# intentional delay or backoff, because it is measured from created_at.
QUEUE_WAIT = Histogram(
    "jobqueue_queue_wait_seconds",
    "Time from enqueue to the start of a delivery, including delay and backoff.",
    ["job_type", "priority"],
    buckets=(
        0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600,
        1800, 3600,
    ),
)

# Enqueue-to-successful-completion wall time. Observed only when the ack wins.
JOB_TOTAL_TIME = Histogram(
    "jobqueue_job_total_seconds",
    "Time from enqueue to successful completion, by job type and priority.",
    ["job_type", "priority"],
    buckets=(
        0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600,
        1800, 3600,
    ),
)

# The backoff delay chosen for a handler-failure retry. Visibility-timeout
# requeues are immediate and are counted by REAPED instead.
RETRY_DELAY = Histogram(
    "jobqueue_retry_delay_seconds",
    "Backoff delay scheduled before the next handler retry, by job type.",
    ["job_type"],
    buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300),
)

# Exception class names only. Never exception text or payload values.
HANDLER_ERRORS = Counter(
    "jobqueue_handler_errors_total",
    "Handler exceptions, by job type and exception class name.",
    ["job_type", "error_type"],
)

# Visibility-timeout actions taken by this worker's reaper thread. The Lua
# script guarantees each expired job is handled once, so summing across
# replicas gives an accurate cluster-wide event count.
REAPED = Counter(
    "jobqueue_reaped_total",
    "Jobs handled after a visibility timeout, by action.",
    ["action"],  # requeued | dead
)

# Errors inside the reaper or promoter loop. These are process-local health
# signals; a crashed worker disappears from up{} instead.
BACKGROUND_ERRORS = Counter(
    "jobqueue_background_errors_total",
    "Errors raised by a worker background loop, by loop name.",
    ["loop"],  # reaper | promoter
)

# 0 or 1 with the current one-job-at-a-time worker model. Summing across
# workers shows live concurrency.
WORKER_INFLIGHT = Gauge(
    "jobqueue_worker_inflight",
    "Jobs currently being executed by this worker process.",
)
