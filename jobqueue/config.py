"""Central configuration, all driven by environment variables.

Everything tunable lives here so the worker, api, and reaper read the same
values. Docker Compose injects these via the environment; defaults are chosen
to make a laptop demo behave sensibly.
"""

import os


def _int(name, default):
    return int(os.environ.get(name, default))


def _float(name, default):
    return float(os.environ.get(name, default))


# Redis connection. In compose this resolves to the "redis" service name.
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

# Priority levels, highest first. A job's priority must be one of these.
# Workers always drain a higher level fully before touching a lower one.
PRIORITIES = ["high", "default", "low"]
DEFAULT_PRIORITY = "default"

# Visibility timeout in seconds. Once a worker claims a job it is hidden from
# other workers for this long. If the worker does not ack within the window the
# reaper assumes it died and makes the job visible again. Keep this comfortably
# larger than your slowest expected job, otherwise healthy jobs get reaped and
# run twice.
VISIBILITY_TIMEOUT = _int("VISIBILITY_TIMEOUT", 30)

# Retry policy. delay = BACKOFF_BASE * (2 ** attempts), capped at BACKOFF_MAX.
# After MAX_ATTEMPTS total tries the job is moved to the dead-letter list.
MAX_ATTEMPTS = _int("MAX_ATTEMPTS", 5)
BACKOFF_BASE = _float("BACKOFF_BASE", 2.0)
BACKOFF_MAX = _float("BACKOFF_MAX", 300.0)

# How often the reaper scans inflight for expired jobs, and the promoter scans
# scheduled for due jobs. Seconds.
REAP_INTERVAL = _float("REAP_INTERVAL", 1.0)
PROMOTE_INTERVAL = _float("PROMOTE_INTERVAL", 1.0)

# Worker idle backoff. When no job is ready the worker sleeps this long before
# polling again, so an empty queue does not spin the CPU.
WORKER_IDLE_SLEEP = _float("WORKER_IDLE_SLEEP", 0.5)

# How long finished job records (completed or dead) live before Redis expires
# them, in seconds. Status stays queryable for a while after completion.
RESULT_TTL = _int("RESULT_TTL", 3600)

# Port the metrics endpoint listens on inside a worker process. Each worker
# container exposes its own so Prometheus can scrape per-worker series.
WORKER_METRICS_PORT = _int("WORKER_METRICS_PORT", 9100)
