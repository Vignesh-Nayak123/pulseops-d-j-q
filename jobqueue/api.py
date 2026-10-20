"""Flask REST API for the job queue.

Endpoints:
  POST /enqueue       submit a job (type, payload, optional priority, delay, key)
  GET  /jobs/<id>     fetch a job's current status and record
  GET  /stats         queue depth, in-flight, completed, failed, etc.
  GET  /healthz       liveness probe, also pings Redis
  GET  /metrics       Prometheus exposition (cluster-wide queue depth)

The API is stateless. All state lives in Redis, so you can run several API
replicas behind a load balancer if you want; this demo runs one.
"""

import time

from flask import Flask, jsonify, request
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest, REGISTRY

from . import config, jobs, queue
from .metrics import QueueDepthCollector
from .redis_client import get_client
from flask_cors import CORS


def create_app():
    app = Flask(__name__)
    CORS(
        app,
        resources={
            r"/*": {
                "origins": [
                    "http://localhost:5173",
                    "http://localhost:5174",
                ]
            }
        },
    )
    # Register the cluster-wide queue-depth collector once, on the API process.
    # Workers expose their own per-process metrics on a separate port instead.
    REGISTRY.register(QueueDepthCollector(queue))

    @app.post("/enqueue")
    def enqueue():
        """Enqueue a job.

        Body (JSON):
          type            required, must match a registered handler.
          payload         optional dict passed to the handler.
          priority        optional, one of the configured priorities.
          delay_seconds   optional, run no earlier than now + this many seconds.
          run_at          optional, absolute unix epoch seconds to run at.
          idempotency_key optional, dedupes repeat submissions.
        """
        body = request.get_json(silent=True) or {}
        job_type = body.get("type")
        if not job_type:
            return jsonify({"error": "missing required field 'type'"}), 400
        if jobs.get_handler(job_type) is None:
            return jsonify({
                "error": "unknown job type %r" % job_type,
                "registered_types": jobs.registered_types(),
            }), 400

        priority = body.get("priority")
        if priority is not None and priority not in config.PRIORITIES:
            return jsonify({
                "error": "invalid priority %r" % priority,
                "valid_priorities": config.PRIORITIES,
            }), 400

        # Resolve an optional delay into an absolute run-at timestamp. run_at
        # wins if both are given.
        delay_until = None
        if body.get("run_at") is not None:
            delay_until = float(body["run_at"])
        elif body.get("delay_seconds") is not None:
            delay_until = time.time() + float(body["delay_seconds"])

        try:
            job_id, created = queue.enqueue(
                job_type=job_type,
                payload=body.get("payload"),
                priority=priority,
                delay_until=delay_until,
                idempotency_key=body.get("idempotency_key"),
            )
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

        # 201 for a freshly created job, 200 when an idempotency key matched an
        # existing one so the caller can tell the difference.
        return jsonify({"id": job_id, "created": created}), (201 if created else 200)

    @app.get("/jobs/<job_id>")
    def get_job(job_id):
        job = queue.get_job(job_id)
        if job is None:
            return jsonify({"error": "job not found", "id": job_id}), 404
        return jsonify(job)

    @app.get("/stats")
    def stats():
        return jsonify(queue.stats())

    @app.get("/healthz")
    def healthz():
        try:
            get_client().ping()
        except Exception as exc:  # surface a bad Redis as unhealthy
            return jsonify({"status": "unhealthy", "redis_error": str(exc)}), 503
        return jsonify({"status": "ok"})

    @app.get("/metrics")
    def metrics():
        return generate_latest(REGISTRY), 200, {"Content-Type": CONTENT_TYPE_LATEST}

    return app


# Module-level app so gunicorn can import it as "jobqueue.api:app".
app = create_app()
