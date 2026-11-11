"""Prometheus helpers, API request metrics, and the cluster-wide collector.

Two kinds of observability live here:

1. The API request metrics, which are registered only in the API process.
2. ``QueueDepthCollector``, which reads live queue state from Redis at scrape
   time. It is registered by the API only, so queue depth and the lifetime
   counters derived from Redis are reported once for the whole cluster instead
   of being duplicated by every worker.

Worker-only metrics live in :mod:`jobqueue.worker_metrics` so importing the API
does not register a phantom ``jobqueue_worker_inflight`` series for the API
process. That distinction keeps ``count(up{job="jobqueue-workers"})`` and
``sum(jobqueue_worker_inflight)`` honest when Prometheus scrapes both roles.
"""

import re

from prometheus_client import Counter, Histogram
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily

from . import config, jobs

# --- Bounded label helpers ---------------------------------------------------

_METHODS = frozenset({"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"})
_SAFE_LABEL_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")


def method_label(method):
    """Return a bounded HTTP method label.

    Flask normally only sees standard methods, but mapping anything else to
    ``OTHER`` keeps accidental custom verbs from becoming a label value.
    """
    if not method:
        return "OTHER"
    method = str(method).upper()
    return method if method in _METHODS else "OTHER"


def job_type_label(job_type):
    """Return a bounded job-type label.

    Registered handlers are the only source of normal job types. Anything else
    (for example a job inserted directly into Redis) is folded into
    ``unregistered`` so arbitrary values cannot create new series.
    """
    if isinstance(job_type, str) and jobs.get_handler(job_type) is not None:
        return job_type
    return "unregistered"


def priority_label(priority):
    """Return a configured priority label, or ``unknown``."""
    return priority if priority in config.PRIORITIES else "unknown"


def error_type_label(exc):
    """Return the exception class name, never the exception message.

    Class names are short identifiers in normal Python code. The regex is a
    belt-and-braces guard against a dynamically named exception class turning
    into an unbounded label value.
    """
    name = type(exc).__name__
    return name if _SAFE_LABEL_RE.match(name) else "exception"


# --- API request metrics (API process only) ---------------------------------

API_REQUESTS = Counter(
    "jobqueue_api_requests_total",
    "HTTP requests served by the API, by method, route pattern, and status.",
    ["method", "path", "status"],
)

API_REQUEST_LATENCY = Histogram(
    "jobqueue_api_request_duration_seconds",
    "API request latency in seconds, by method and route pattern.",
    ["method", "path"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)


# --- Cluster-wide queue collector (API process only) ------------------------

class QueueDepthCollector:
    """Custom collector that queries Redis on each scrape and reports current
    queue sizes plus the lifetime counters maintained by the Lua scripts.

    The collector is registered in the API process only. Redis is polled once
    per Prometheus scrape (5 seconds in the provided configuration), which keeps
    the gauges fresh even when no jobs are moving while avoiding a background
    polling loop.
    """

    def __init__(self, queue_module):
        # Passed in to avoid an import cycle (queue imports config, not metrics).
        self._queue = queue_module

    def collect(self):
        try:
            stats = self._queue.stats()
        except Exception:
            # Keep the endpoint scrapeable when Redis is unavailable. The rest
            # of the series disappear rather than serving stale queue depths.
            yield GaugeMetricFamily(
                "jobqueue_redis_up",
                "1 if the API could read queue state from Redis, else 0.",
                value=0,
            )
            return

        yield GaugeMetricFamily(
            "jobqueue_redis_up",
            "1 if the API could read queue state from Redis, else 0.",
            value=1,
        )

        depth = GaugeMetricFamily(
            "jobqueue_queue_depth",
            "Number of jobs in a queue right now, by state.",
            labels=["state"],
        )
        # Only ready queues are priority-indexed in Redis. The other states are
        # reported as single cluster-wide values.
        for priority in config.PRIORITIES:
            depth.add_metric(
                ["ready:" + priority],
                stats["ready"].get(priority, 0),
            )
        depth.add_metric(["ready_total"], stats["ready_total"])
        depth.add_metric(["scheduled"], stats["scheduled"])
        depth.add_metric(["scheduled_due"], stats.get("scheduled_due", 0))
        depth.add_metric(["inflight"], stats["inflight"])
        depth.add_metric(["dead"], stats["dead"])
        yield depth

        counters = (
            (
                "jobqueue_jobs_enqueued_total",
                "New jobs created by enqueue calls. Idempotent dedupes are excluded.",
                stats.get("enqueued", 0),
            ),
            (
                "jobqueue_jobs_completed_total",
                "Jobs acknowledged as completed by workers.",
                stats.get("completed", 0),
            ),
            (
                "jobqueue_failed_attempts_total",
                "Failed delivery attempts, including handler failures and visibility timeouts.",
                stats.get("failed", 0),
            ),
            (
                "jobqueue_jobs_retried_total",
                "Handler failures scheduled for a backoff retry.",
                stats.get("retried", 0),
            ),
            (
                "jobqueue_dead_letters_total",
                "Jobs moved to the dead-letter list after exhausting attempts.",
                stats.get("dead_letters", 0),
            ),
            (
                "jobqueue_jobs_promoted_total",
                "Scheduled or backed-off jobs moved into a ready queue by the promoter.",
                stats.get("promoted", 0),
            ),
            (
                "jobqueue_idempotency_dedup_total",
                "Enqueue requests deduplicated by an idempotency key.",
                stats.get("deduplicated", 0),
            ),
        )
        for name, help_text, value in counters:
            family = CounterMetricFamily(name, help_text)
            family.add_metric([], value)
            yield family
