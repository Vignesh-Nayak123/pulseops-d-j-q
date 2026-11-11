"""Job handler registry.

A handler is just a function that takes the job payload (a dict) and does the
work. It returns normally on success and raises on failure; the worker turns a
raised exception into a retry or a dead-letter via the backoff policy.

Register handlers with the @handler decorator keyed by job type. The worker
looks up the type recorded on the job and calls the matching function.
"""

import csv
import math
import os
import random
import re
import smtplib
import ssl
import time
from email.message import EmailMessage

from . import config

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


@handler("text_stats")
def text_stats_job(payload):
    """Return character, word, and line counts for payload['text'].

    Lines are counted by newline separators: empty text has 0 lines, and
    otherwise lines = number of "\\n" characters + 1. The text itself is never
    included in the result or logged.
    """
    text = payload.get("text")
    if not isinstance(text, str):
        raise ValueError("text_stats requires 'text' to be a string")
    return {
        "characters": len(text),
        "words": len(text.split()),
        "lines": 0 if text == "" else text.count("\n") + 1,
        "characters_without_spaces": len(text.replace(" ", "")),
    }


_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+$")


def _validated_email(value, field):
    """Return a normalized address or raise a clear ValueError."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError("%s is required and must be a non-empty email address" % field)
    address = value.strip()
    if "\r" in address or "\n" in address or not _EMAIL_RE.match(address):
        raise ValueError("%s must be a valid email address" % field)
    local, domain = address.rsplit("@", 1)
    if (
        local.startswith(".") or local.endswith(".") or ".." in local
        or domain.startswith(".") or domain.endswith(".") or ".." in domain
    ):
        raise ValueError("%s must be a valid email address" % field)
    return address


@handler("send_email")
def send_email_job(payload):
    """Send an email through the configured SMTP server.

    payload:
      to       recipient address (required)
      subject  subject line (required, string)
      body     plain-text message body (required, string)

    SMTP settings come from config/environment. Delivery failures raise so the
    worker's normal retry and dead-letter policy applies. Because delivery is
    at-least-once, a retry can send a duplicate if the SMTP server accepted the
    message but the worker died before acking the job.
    """
    to = _validated_email(payload.get("to"), "to")

    subject = payload.get("subject")
    if not isinstance(subject, str):
        raise ValueError("subject is required and must be a string")
    if "\r" in subject or "\n" in subject:
        raise ValueError("subject must not contain newline characters")

    body = payload.get("body")
    if not isinstance(body, str):
        raise ValueError("body is required and must be a string")

    sender = _validated_email(config.SMTP_FROM, "SMTP_FROM")
    if not config.SMTP_HOST:
        raise ValueError("SMTP_HOST must be configured")
    if not 0 < config.SMTP_PORT <= 65535:
        raise ValueError("SMTP_PORT must be between 1 and 65535")
    if not math.isfinite(config.SMTP_TIMEOUT) or config.SMTP_TIMEOUT <= 0:
        raise ValueError("SMTP_TIMEOUT must be a finite positive number of seconds")

    message = EmailMessage()
    message["From"] = sender
    message["To"] = to
    message["Subject"] = subject
    message.set_content(body)

    with smtplib.SMTP(
        config.SMTP_HOST, config.SMTP_PORT, timeout=config.SMTP_TIMEOUT
    ) as smtp:
        if config.SMTP_STARTTLS:
            smtp.starttls(context=ssl.create_default_context())
            smtp.ehlo()
        if config.SMTP_USERNAME:
            smtp.login(config.SMTP_USERNAME, config.SMTP_PASSWORD)
        smtp.send_message(message)

    # Do not return or log the subject/body. The envelope recipient is enough
    # to identify the send without copying message content into the job record.
    return {"sent_to": to}


_MAX_REPORT_ROWS = 100000
_REPORT_NAME_RE = re.compile(r"[^A-Za-z0-9_-]+")
_REPORT_CUSTOMERS = ["Ada Lovelace", "Grace Hopper", "Alan Turing", "Katherine Johnson"]
_REPORT_PRODUCTS = [
    ("Notebook", "12.50"),
    ("Pen", "2.25"),
    ("Monitor", "189.99"),
    ("Keyboard", "49.00"),
]
_REPORT_STATUSES = ["paid", "pending", "shipped", "refunded"]


@handler("generate_report")
def generate_report_job(payload):
    """Write deterministic sample order data as a CSV file.

    payload:
      report_name  optional label used in the filename (default "orders")
      rows         optional row count, 1..100000 (default 100)

    The filename includes the job id so concurrent jobs cannot overwrite each
    other. Invalid input raises ValueError, which the worker's existing retry
    and dead-letter policy handles like any other handler failure.
    """
    report_name = str(payload.get("report_name") or "orders")
    safe_name = _REPORT_NAME_RE.sub("_", report_name).strip("._-")[:50]
    if not safe_name:
        safe_name = "orders"

    raw_rows = payload.get("rows", 100)
    if isinstance(raw_rows, bool):
        raise ValueError("rows must be an integer between 1 and %d" % _MAX_REPORT_ROWS)
    try:
        rows = int(raw_rows)
    except (TypeError, ValueError):
        raise ValueError("rows must be an integer between 1 and %d" % _MAX_REPORT_ROWS)
    if isinstance(raw_rows, float) and not raw_rows.is_integer():
        raise ValueError("rows must be an integer between 1 and %d" % _MAX_REPORT_ROWS)
    if not 1 <= rows <= _MAX_REPORT_ROWS:
        raise ValueError("rows must be between 1 and %d" % _MAX_REPORT_ROWS)

    # The worker adds the real job id to the payload so the filename stays
    # unique per submission. Keep this defensive for direct calls.
    safe_job_id = re.sub(r"[^A-Za-z0-9_-]", "", str(payload.get("_job_id", ""))) or "job"
    filename = "%s-%s.csv" % (safe_name, safe_job_id)

    os.makedirs(config.REPORTS_DIR, exist_ok=True)
    path = os.path.join(config.REPORTS_DIR, filename)

    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "order_id", "customer", "product", "quantity", "unit_price",
            "status", "created_at",
        ])
        for index in range(1, rows + 1):
            product, unit_price = _REPORT_PRODUCTS[(index - 1) % len(_REPORT_PRODUCTS)]
            writer.writerow([
                1000 + index,
                _REPORT_CUSTOMERS[(index - 1) % len(_REPORT_CUSTOMERS)],
                product,
                (index % 5) + 1,
                unit_price,
                _REPORT_STATUSES[index % len(_REPORT_STATUSES)],
                "2026-01-%02dT%02d:00:00Z" % (((index - 1) % 28) + 1, (index - 1) % 24),
            ])

    return {"filename": filename, "rows": rows, "path": path}
