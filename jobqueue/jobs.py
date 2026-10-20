"""Job handler registry.

A handler is just a function that takes the job payload (a dict) and does the
work. It returns normally on success and raises on failure; the worker turns a
raised exception into a retry or a dead-letter via the backoff policy.

Register handlers with the @handler decorator keyed by job type. The worker
looks up the type recorded on the job and calls the matching function.
"""

import random
import time

# job type -> callable(payload) registry.
_HANDLERS = {}


def handler(job_type):
    def register(fn):
        _HANDLERS[job_type] = fn
        return fn
    return register


def get_handler(job_type):
    """Return the handler for a job type, or None if nothing is registered."""
    return _HANDLERS.get(job_type)


def registered_types():
    return sorted(_HANDLERS.keys())


# ---------------------------------------------------------------------------
# Sample handlers shipped with the demo.
# ---------------------------------------------------------------------------

@handler("sleep")
def sleep_job(payload):
    """Simulated work: sleep for payload['seconds'] (default 1). Useful for load
    tests and for watching the queue drain in Grafana at a controlled rate."""
    seconds = float(payload.get("seconds", 1))
    time.sleep(seconds)
    return {"slept": seconds}


@handler("flaky")
def flaky_job(payload):
    """Fails with probability payload['fail_rate'] (default 0.5). Demonstrates
    retry, exponential backoff, and the dead-letter queue. The randomness is
    intentional, so a given job may succeed on a later attempt."""
    fail_rate = float(payload.get("fail_rate", 0.5))
    if random.random() < fail_rate:
        raise RuntimeError("flaky job rolled a failure (fail_rate=%s)" % fail_rate)
    return {"ok": True}


@handler("email")
def email_job(payload):
    """Mock email send. A recognizable real-world job: in production this would
    call an SMTP or provider API. Here it just validates and logs."""
    to = payload.get("to")
    if not to:
        raise ValueError("email job requires a 'to' address")
    subject = payload.get("subject", "(no subject)")
    # Stand-in for the real send. Printed so it shows up in the worker logs.
    print("[email] sending to=%s subject=%r" % (to, subject), flush=True)
    return {"sent_to": to}
