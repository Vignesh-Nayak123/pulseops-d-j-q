"""Distributed job queue backed by Redis.

Package layout:
  config.py        env-driven settings, read once at import.
  redis_client.py  shared connection factory.
  queue.py         the core: enqueue, claim, ack, fail, promote, reap.
  jobs.py          handler registry mapping job type -> python function.
  metrics.py       Prometheus collectors shared by api and worker.
  api.py           Flask REST app.
  worker.py        worker loop plus the background reaper and promoter.
"""

__version__ = "1.0.0"
