import unittest
from unittest import mock

from prometheus_client import REGISTRY

from jobqueue import api


def _stats():
    return {
        "ready": {"high": 0, "default": 0, "low": 0},
        "ready_total": 0,
        "scheduled": 0,
        "scheduled_due": 0,
        "inflight": 0,
        "dead": 0,
        "enqueued": 0,
        "completed": 0,
        "failed": 0,
        "retried": 0,
        "dead_letters": 0,
        "promoted": 0,
        "deduplicated": 0,
    }


def _value(name, labels):
    return REGISTRY.get_sample_value(name, labels)


class ApiRequestMetricTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = api.create_app()
        cls.client = cls.app.test_client()

    def test_job_id_is_not_used_as_a_path_label(self):
        with mock.patch.object(api.queue, "get_job", return_value=None):
            response = self.client.get("/jobs/abc123")

        self.assertEqual(response.status_code, 404)
        self.assertGreaterEqual(
            _value(
                "jobqueue_api_requests_total",
                {"method": "GET", "path": "/jobs/<job_id>", "status": "404"},
            ),
            1,
        )
        self.assertIsNone(
            _value(
                "jobqueue_api_requests_total",
                {"method": "GET", "path": "/jobs/abc123", "status": "404"},
            )
        )

    def test_enqueue_validation_errors_are_labelled_by_route_and_status(self):
        response = self.client.post("/enqueue", json={})

        self.assertEqual(response.status_code, 400)
        self.assertGreaterEqual(
            _value(
                "jobqueue_api_requests_total",
                {"method": "POST", "path": "/enqueue", "status": "400"},
            ),
            1,
        )

    def test_metrics_scrape_is_excluded_from_api_request_metrics(self):
        with mock.patch.object(api.queue, "stats", return_value=_stats()):
            response = self.client.get("/metrics")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn("jobqueue_queue_depth", body)
        self.assertIn("jobqueue_redis_up", body)
        self.assertIsNone(
            _value(
                "jobqueue_api_requests_total",
                {"method": "GET", "path": "/metrics", "status": "200"},
            )
        )

    def test_create_app_can_be_called_more_than_once(self):
        # The collector registration guard matters for tests and for any future
        # factory reuse in one process.
        api.create_app()


if __name__ == "__main__":
    unittest.main()
