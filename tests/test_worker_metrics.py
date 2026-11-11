import time
import unittest
from unittest import mock

from prometheus_client import REGISTRY

from jobqueue import jobs, metrics, worker, worker_metrics


@jobs.handler("test_metric_ok")
def _test_ok(payload):
    return {"ok": True}


@jobs.handler("test_metric_fail")
def _test_fail(payload):
    raise ValueError("handler failed")


@jobs.handler("test_metric_lost")
def _test_lost(payload):
    return {"ok": True}


def _job(job_type, priority="high", attempts=0):
    return {
        "type": job_type,
        "payload": {},
        "priority": priority,
        "created_at": time.time() - 1,
        "attempts": attempts,
    }


def _value(name, labels=None):
    return REGISTRY.get_sample_value(name, labels or {})


class WorkerProcessMetricTests(unittest.TestCase):
    def test_successful_delivery_records_outcome_wait_and_total_time(self):
        with mock.patch.object(worker.queue, "get_job", return_value=_job("test_metric_ok")), \
             mock.patch.object(worker.queue, "ack", return_value=True):
            worker._process("job-ok")

        self.assertEqual(
            _value(
                "jobqueue_jobs_processed_total",
                {"job_type": "test_metric_ok", "outcome": "completed", "priority": "high"},
            ),
            1,
        )
        self.assertEqual(
            _value(
                "jobqueue_job_processing_seconds_count",
                {"job_type": "test_metric_ok", "outcome": "completed"},
            ),
            1,
        )
        self.assertEqual(
            _value(
                "jobqueue_queue_wait_seconds_count",
                {"job_type": "test_metric_ok", "priority": "high"},
            ),
            1,
        )
        self.assertEqual(
            _value(
                "jobqueue_job_total_seconds_count",
                {"job_type": "test_metric_ok", "priority": "high"},
            ),
            1,
        )
        self.assertEqual(_value("jobqueue_worker_inflight"), 0)

    def test_handler_failure_records_error_retry_and_delay(self):
        with mock.patch.object(worker.queue, "get_job", return_value=_job("test_metric_fail", priority="low")), \
             mock.patch.object(worker.queue, "fail", return_value="retry") as fail:
            worker._process("job-fail")

        fail.assert_called_once()
        self.assertEqual(
            _value(
                "jobqueue_jobs_processed_total",
                {"job_type": "test_metric_fail", "outcome": "retry", "priority": "low"},
            ),
            1,
        )
        self.assertEqual(
            _value(
                "jobqueue_handler_errors_total",
                {"job_type": "test_metric_fail", "error_type": "ValueError"},
            ),
            1,
        )
        self.assertEqual(
            _value(
                "jobqueue_retry_delay_seconds_count",
                {"job_type": "test_metric_fail"},
            ),
            1,
        )
        self.assertEqual(
            _value(
                "jobqueue_retry_delay_seconds_sum",
                {"job_type": "test_metric_fail"},
            ),
            2.0,
        )
        self.assertIsNone(
            _value(
                "jobqueue_job_total_seconds_count",
                {"job_type": "test_metric_fail", "priority": "low"},
            )
        )

    def test_lost_lease_is_not_counted_as_completion(self):
        with mock.patch.object(worker.queue, "get_job", return_value=_job("test_metric_lost")), \
             mock.patch.object(worker.queue, "ack", return_value=False):
            worker._process("job-lost")

        self.assertEqual(
            _value(
                "jobqueue_jobs_processed_total",
                {"job_type": "test_metric_lost", "outcome": "lost", "priority": "high"},
            ),
            1,
        )
        self.assertIsNone(
            _value(
                "jobqueue_job_total_seconds_count",
                {"job_type": "test_metric_lost", "priority": "high"},
            )
        )

    def test_unknown_job_type_is_collapsed_to_a_bounded_label(self):
        with mock.patch.object(worker.queue, "get_job", return_value=_job("test_metric_unknown")), \
             mock.patch.object(worker.queue, "fail", return_value="retry"):
            worker._process("job-unknown")

        self.assertEqual(
            _value(
                "jobqueue_jobs_processed_total",
                {"job_type": "unregistered", "outcome": "retry", "priority": "high"},
            ),
            1,
        )
        self.assertEqual(
            _value(
                "jobqueue_handler_errors_total",
                {"job_type": "unregistered", "error_type": "RuntimeError"},
            ),
            1,
        )


class WorkerMetricSeparationTests(unittest.TestCase):
    def test_api_metrics_module_does_not_define_worker_series(self):
        self.assertFalse(hasattr(metrics, "WORKER_INFLIGHT"))
        self.assertFalse(hasattr(metrics, "JOBS_PROCESSED"))

    def test_worker_metrics_have_bounded_label_sets(self):
        self.assertEqual(
            worker_metrics.JOBS_PROCESSED._labelnames,
            ("job_type", "outcome", "priority"),
        )
        self.assertEqual(
            worker_metrics.REAPED._labelnames,
            ("action",),
        )
        self.assertEqual(
            worker_metrics.BACKGROUND_ERRORS._labelnames,
            ("loop",),
        )


if __name__ == "__main__":
    unittest.main()
