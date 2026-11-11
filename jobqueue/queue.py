"""Core queue operations on top of Redis.

This module owns the key layout and loads the Lua scripts that provide the
atomic guarantees. Everything else (api, worker, reaper) calls into here.

Key layout:
  job:<id>               HASH   full job record.
  queue:ready:<priority> ZSET   ready job ids, scored by an increasing sequence
                                number so smallest score == oldest == FIFO.
  queue:scheduled        ZSET   job ids scored by run-at epoch (delayed enqueues
                                and backoff retries both live here).
  queue:inflight         ZSET   claimed job ids scored by visibility deadline.
  queue:dead             LIST   dead-letter job ids.
  queue:seq              STRING monotonic counter used to score ready queues.
  idempotency:<key>      STRING -> job id, written with NX so a duplicate
                                enqueue becomes a no-op.
  stats:enqueued         STRING  new jobs created (dedupes excluded).
  stats:completed        STRING  jobs acknowledged as completed.
  stats:failed           STRING  failed delivery attempts (handler failures
                                 and visibility timeouts).
  stats:retried          STRING  handler failures scheduled for backoff retry.
  stats:dead             STRING  jobs moved to the dead-letter list.
  stats:promoted         STRING  scheduled jobs moved into a ready queue.
  stats:deduplicated     STRING  idempotent enqueue requests deduplicated.
"""

import json
import os
import time
import uuid

from . import config
from .redis_client import get_client

# Key builders, kept in one place so every module agrees on the names.
JOB_PREFIX = "job:"
READY_PREFIX = "queue:ready:"
SCHEDULED_KEY = "queue:scheduled"
INFLIGHT_KEY = "queue:inflight"
DEAD_KEY = "queue:dead"
SEQ_KEY = "queue:seq"
IDEMPOTENCY_PREFIX = "idempotency:"
STAT_ENQUEUED = "stats:enqueued"
STAT_COMPLETED = "stats:completed"
STAT_FAILED = "stats:failed"
STAT_RETRIED = "stats:retried"
STAT_DEAD_LETTERS = "stats:dead"
STAT_PROMOTED = "stats:promoted"
STAT_DEDUPLICATED = "stats:deduplicated"


def _job_key(job_id):
    return JOB_PREFIX + job_id


def _ready_key(priority):
    return READY_PREFIX + priority


def _idempotency_key(key):
    return IDEMPOTENCY_PREFIX + key


# Load the Lua scripts from disk once at import. register_script returns a
# callable that uses EVALSHA under the hood and falls back to EVAL if the script
# is not cached on the server yet.
_LUA_DIR = os.path.join(os.path.dirname(__file__), "lua")


def _load_script(name):
    with open(os.path.join(_LUA_DIR, name), "r", encoding="utf-8") as fh:
        return get_client().register_script(fh.read())


_claim_script = _load_script("claim.lua")
_ack_script = _load_script("ack.lua")
_fail_script = _load_script("fail.lua")
_promote_script = _load_script("promote.lua")
_reap_script = _load_script("reap.lua")


def _now():
    return time.time()


def enqueue(job_type, payload=None, priority=None, delay_until=None, idempotency_key=None):
    """Add a job to the queue.

    Returns (job_id, created) where created is False when an idempotency key
    matched an existing job, in which case the existing id is returned and
    nothing new is enqueued.
    """
    r = get_client()
    priority = priority or config.DEFAULT_PRIORITY
    if priority not in config.PRIORITIES:
        raise ValueError("unknown priority %r, expected one of %s" % (priority, config.PRIORITIES))

    # Idempotency: reserve the key with SET NX before doing any work. If another
    # request already reserved it, return the job that request created. We use a
    # placeholder uuid up front so the stored value points at the real job id.
    job_id = uuid.uuid4().hex
    if idempotency_key:
        reserved = r.set(_idempotency_key(idempotency_key), job_id, nx=True, ex=config.RESULT_TTL)
        if not reserved:
            existing = r.get(_idempotency_key(idempotency_key))
            # existing should be set, but guard against a race where it expired
            # between our SET and GET by falling through to a fresh enqueue.
            if existing:
                r.incr(STAT_DEDUPLICATED)
                return existing, False

    now = _now()
    run_at = float(delay_until) if delay_until else now

    job = {
        "id": job_id,
        "type": job_type,
        "payload": json.dumps(payload or {}),
        "priority": priority,
        "status": "scheduled" if run_at > now else "ready",
        "attempts": 0,
        "created_at": now,
        "run_at": run_at,
        "idempotency_key": idempotency_key or "",
    }
    # Pipeline the writes so the hash and the queue insert go in one round trip.
    pipe = r.pipeline()
    pipe.hset(_job_key(job_id), mapping=job)
    pipe.expire(_job_key(job_id), config.RESULT_TTL * 24)  # generous ttl while live
    if run_at > now:
        # Future job: park in scheduled, the promoter moves it when due.
        pipe.zadd(SCHEDULED_KEY, {job_id: run_at})
    else:
        # Ready now: score by the next sequence number for FIFO ordering.
        seq = r.incr(SEQ_KEY)
        pipe.zadd(_ready_key(priority), {job_id: seq})
    pipe.incr(STAT_ENQUEUED)
    pipe.execute()
    return job_id, True


def get_job(job_id):
    """Return the job record as a dict, or None if unknown/expired."""
    raw = get_client().hgetall(_job_key(job_id))
    if not raw:
        return None
    # Decode the fields the API and callers care about back into native types.
    raw["payload"] = json.loads(raw.get("payload", "{}"))
    if raw.get("result"):
        raw["result"] = json.loads(raw["result"])
    for field in ("attempts",):
        if field in raw:
            raw[field] = int(raw[field])
    for field in ("created_at", "run_at", "claimed_at", "completed_at", "failed_at"):
        if field in raw:
            raw[field] = float(raw[field])
    return raw


def claim():
    """Atomically claim the next ready job. Returns a job id or None.

    KEYS are the ready queues in priority order followed by the inflight key,
    matching claim.lua's contract.
    """
    now = _now()
    deadline = now + config.VISIBILITY_TIMEOUT
    keys = [_ready_key(p) for p in config.PRIORITIES] + [INFLIGHT_KEY]
    result = _claim_script(keys=keys, args=[deadline, now, JOB_PREFIX])
    return result  # job id string, or None when every ready queue was empty


def ack(job_id, result=None):
    """Mark a claimed job completed. Returns True if this worker still owned it.

    The optional handler result is stored as JSON on the job record so callers
    of GET /jobs/<id> can see it. None leaves the record unchanged, preserving
    the existing response shape for handlers that return nothing.
    """
    now = _now()
    result_json = json.dumps(result) if result is not None else ""
    acked = _ack_script(
        keys=[INFLIGHT_KEY, _job_key(job_id), STAT_COMPLETED],
        args=[now, config.RESULT_TTL, job_id, result_json],
    )
    return acked == 1


def fail(job_id, error):
    """Record a failed attempt. fail.lua increments the attempt counter, then
    either schedules a backoff retry or dead-letters the job once it has used up
    MAX_ATTEMPTS. Returns one of 'retry', 'dead', 'lost'."""
    now = _now()
    result = _fail_script(
        keys=[
            INFLIGHT_KEY,
            _job_key(job_id),
            SCHEDULED_KEY,
            DEAD_KEY,
            STAT_FAILED,
            STAT_RETRIED,
            STAT_DEAD_LETTERS,
        ],
        args=[job_id, config.MAX_ATTEMPTS, config.BACKOFF_BASE, config.BACKOFF_MAX,
              now, str(error), config.RESULT_TTL],
    )
    return result


def promote(limit=200):
    """Move due scheduled jobs into their ready queues. Returns count promoted."""
    now = _now()
    return _promote_script(
        keys=[SCHEDULED_KEY, SEQ_KEY, STAT_PROMOTED],
        args=[now, JOB_PREFIX, READY_PREFIX, config.DEFAULT_PRIORITY, limit],
    )


def reap(limit=200):
    """Requeue jobs whose visibility lease expired. Returns (requeued, dead)."""
    now = _now()
    requeued, dead = _reap_script(
        keys=[INFLIGHT_KEY, SEQ_KEY, DEAD_KEY, STAT_FAILED, STAT_DEAD_LETTERS],
        args=[now, JOB_PREFIX, READY_PREFIX, config.DEFAULT_PRIORITY,
              config.MAX_ATTEMPTS, config.RESULT_TTL, limit],
    )
    return requeued, dead


def retry_delay_seconds(attempts_before):
    """Return the backoff delay fail.lua will schedule for the next retry.

    ``attempts_before`` is the attempt count read before the failed delivery is
    recorded. fail.lua increments the counter first, then uses
    ``BACKOFF_BASE * 2 ** (attempts - 1)``, which is the same value as
    ``BACKOFF_BASE * 2 ** attempts_before`` here.
    """
    try:
        attempts_before = max(0, int(attempts_before))
    except (TypeError, ValueError):
        attempts_before = 0
    return min(config.BACKOFF_BASE * (2 ** attempts_before), config.BACKOFF_MAX)


def stats():
    """Snapshot of queue depth and lifetime counters for /stats and /metrics."""
    r = get_client()
    now = _now()
    pipe = r.pipeline()
    for p in config.PRIORITIES:
        pipe.zcard(_ready_key(p))
    pipe.zcard(SCHEDULED_KEY)
    pipe.zcard(INFLIGHT_KEY)
    pipe.llen(DEAD_KEY)
    pipe.zcount(SCHEDULED_KEY, "-inf", now)
    pipe.get(STAT_ENQUEUED)
    pipe.get(STAT_COMPLETED)
    pipe.get(STAT_FAILED)
    pipe.get(STAT_RETRIED)
    pipe.get(STAT_DEAD_LETTERS)
    pipe.get(STAT_PROMOTED)
    pipe.get(STAT_DEDUPLICATED)
    results = pipe.execute()

    n = len(config.PRIORITIES)
    ready_by_priority = {p: results[i] for i, p in enumerate(config.PRIORITIES)}
    (
        scheduled,
        inflight,
        dead,
        scheduled_due,
        enqueued,
        completed,
        failed,
        retried,
        dead_letters,
        promoted,
        deduplicated,
    ) = results[n:]
    return {
        "ready": ready_by_priority,
        "ready_total": sum(ready_by_priority.values()),
        "scheduled": scheduled,
        "scheduled_due": int(scheduled_due or 0),
        "inflight": inflight,
        "dead": dead,
        "enqueued": int(enqueued or 0),
        "completed": int(completed or 0),
        "failed": int(failed or 0),
        "retried": int(retried or 0),
        "dead_letters": int(dead_letters or 0),
        "promoted": int(promoted or 0),
        "deduplicated": int(deduplicated or 0),
    }
