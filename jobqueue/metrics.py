"""Prometheus metrics shared by the API and the workers.

Two kinds of metrics live here:

1. Worker-side counters and histograms (jobs processed, processing latency,
   failures). Each worker process maintains its own series; Prometheus scrapes
   every worker and the dashboard sums or rates across them.

2. A queue-depth collector that reads live counts from Redis at scrape time.
   This is exposed by the API process only, so depth/backlog numbers are
   reported once for the whole cluster instead of duplicated per worker.
"""

from prometheus_client import Counter, Histogram, Gauge
from prometheus_client.core import GaugeMetricFamily

from . import config

# --- Worker-side metrics ----------------------------------------------------

# Total jobs the worker finished, split by type and outcome. rate() of this in
# Grafana gives throughput (jobs/sec) and the failure rate.
JOBS_PROCESSED = Counter(
    "jobqueue_jobs_processed_total",
    "Jobs processed by workers, by type and outcome.",
    ["job_type", "outcome"],  # outcome: completed | retry | dead | lost
)

# End-to-end handler execution time. Buckets cover sub-second jobs up to slow
# multi-second work, which matches the sample handlers and typical load tests.
JOB_LATENCY = Histogram(
    "jobqueue_job_processing_seconds",
    "Handler execution time in seconds, by job type.",
    ["job_type"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60),
)

# Jobs requeued or dead-lettered by the reaper, so timeouts are visible.
REAPED = Counter(
    "jobqueue_reaped_total",
    "Jobs handled by the reaper after a visibility timeout, by action.",
    ["action"],  # requeued | dead
)

# How many jobs this particular worker is running right now (0 or 1 given the
# one-job-at-a-time model, but summing across workers shows live concurrency).
WORKER_INFLIGHT = Gauge(
    "jobqueue_worker_inflight",
    "Jobs currently being executed by this worker process.",
)


# --- Cluster-wide queue depth collector (API process) -----------------------

class QueueDepthCollector:
    """Custom collector that queries Redis on each scrape and reports current
    queue sizes. Registered in the API process only."""

    def __init__(self, queue_module):
        # Passed in to avoid an import cycle (queue imports config, not metrics).
        self._queue = queue_module

    def collect(self):
        stats = self._queue.stats()

        depth = GaugeMetricFamily(
            "jobqueue_queue_depth",
            "Number of jobs in a queue right now, by state.",
            labels=["state"],
        )
        # Per-priority ready depth, so the dashboard can show high vs low.
        for priority, count in stats["ready"].items():
            depth.add_metric(["ready:" + priority], count)
        depth.add_metric(["ready_total"], stats["ready_total"])
        depth.add_metric(["scheduled"], stats["scheduled"])
        depth.add_metric(["inflight"], stats["inflight"])
        depth.add_metric(["dead"], stats["dead"])
        yield depth

        totals = GaugeMetricFamily(
            "jobqueue_jobs_total",
            "Monotonic lifetime counters maintained in Redis.",
            labels=["kind"],
        )
        totals.add_metric(["enqueued"], stats["enqueued"])
        totals.add_metric(["completed"], stats["completed"])
        totals.add_metric(["failed"], stats["failed"])
        yield totals
