import re
import unittest

from prometheus_client import REGISTRY

from jobqueue import metrics, worker_metrics  # noqa: F401 - registers worker series


def _stats(**overrides):
    base = {
        "ready": {"high": 2, "default": 3, "low": 1},
        "ready_total": 6,
        "scheduled": 4,
        "scheduled_due": 1,
        "inflight": 2,
        "dead": 5,
        "enqueued": 100,
        "completed": 80,
        "failed": 12,
        "retried": 7,
        "dead_letters": 3,
        "promoted": 9,
        "deduplicated": 4,
    }
    base.update(overrides)
    return base


def _samples(families):
    samples = {}
    for family in families:
        for sample in family.samples:
            samples[sample.name] = sample
    return samples


class FakeQueue:
    def __init__(self, stats=None, error=None):
        self._stats = stats or _stats()
        self._error = error
        self.calls = 0

    def stats(self):
        self.calls += 1
        if self._error is not None:
            raise self._error
        return self._stats


class QueueDepthCollectorTests(unittest.TestCase):
    def test_reports_all_queue_states_and_counters(self):
        queue = FakeQueue()
        collector = metrics.QueueDepthCollector(queue)

        families = list(collector.collect())
        samples = _samples(families)

        self.assertEqual(samples["jobqueue_redis_up"].value, 1)
        depth_family = next(f for f in families if f.name == "jobqueue_queue_depth")
        depth = {
            sample.labels["state"]: sample.value
            for sample in depth_family.samples
        }
        self.assertEqual(depth["ready:high"], 2)
        self.assertEqual(depth["ready:default"], 3)
        self.assertEqual(depth["ready:low"], 1)
        self.assertEqual(depth["ready_total"], 6)
        self.assertEqual(depth["scheduled"], 4)
        self.assertEqual(depth["scheduled_due"], 1)
        self.assertEqual(depth["inflight"], 2)
        self.assertEqual(depth["dead"], 5)

        self.assertEqual(samples["jobqueue_jobs_enqueued_total"].value, 100)
        self.assertEqual(samples["jobqueue_jobs_completed_total"].value, 80)
        self.assertEqual(samples["jobqueue_failed_attempts_total"].value, 12)
        self.assertEqual(samples["jobqueue_jobs_retried_total"].value, 7)
        self.assertEqual(samples["jobqueue_dead_letters_total"].value, 3)
        self.assertEqual(samples["jobqueue_jobs_promoted_total"].value, 9)
        self.assertEqual(samples["jobqueue_idempotency_dedup_total"].value, 4)

    def test_refreshes_redis_state_on_every_scrape(self):
        queue = FakeQueue()
        collector = metrics.QueueDepthCollector(queue)

        list(collector.collect())
        list(collector.collect())

        self.assertEqual(queue.calls, 2)

    def test_reports_redis_down_without_stale_depth(self):
        collector = metrics.QueueDepthCollector(FakeQueue(error=RuntimeError("down")))
        samples = _samples(list(collector.collect()))

        self.assertEqual(samples["jobqueue_redis_up"].value, 0)
        self.assertNotIn("jobqueue_queue_depth", samples)


class LabelHelperTests(unittest.TestCase):
    def test_job_type_is_bounded_to_registered_handlers(self):
        self.assertEqual(metrics.job_type_label("sleep"), "sleep")
        self.assertEqual(metrics.job_type_label("not-a-real-handler"), "unregistered")
        self.assertEqual(metrics.job_type_label(None), "unregistered")

    def test_priority_is_bounded_to_configured_values(self):
        self.assertEqual(metrics.priority_label("high"), "high")
        self.assertEqual(metrics.priority_label("urgent"), "unknown")
        self.assertEqual(metrics.priority_label(None), "unknown")

    def test_method_is_bounded(self):
        self.assertEqual(metrics.method_label("get"), "GET")
        self.assertEqual(metrics.method_label("CUSTOM-VERB"), "OTHER")
        self.assertEqual(metrics.method_label(None), "OTHER")

    def test_exception_label_uses_class_name_only(self):
        self.assertEqual(metrics.error_type_label(ValueError("secret message")), "ValueError")
        self.assertNotIn("secret", metrics.error_type_label(ValueError("secret message")))


class LabelSafetyTests(unittest.TestCase):
    FORBIDDEN_LABELS = {
        "job_id",
        "idempotency_key",
        "payload",
        "email",
        "to",
        "subject",
        "body",
        "last_error",
        "error",
        "exception",
        "message",
        "password",
        "token",
        "credential",
    }
    EMAIL_RE = re.compile(r"[\w.+-]+@[\w.-]+")
    JOB_ID_RE = re.compile(r"^[0-9a-f]{32}$")

    def test_jobqueue_metrics_never_use_unbounded_labels(self):
        for family in REGISTRY.collect():
            if not family.name.startswith("jobqueue_"):
                continue
            for sample in family.samples:
                for label in sample.labels:
                    self.assertNotIn(label, self.FORBIDDEN_LABELS, family.name)
                for value in sample.labels.values():
                    self.assertIsNone(self.EMAIL_RE.search(value), family.name)
                    self.assertIsNone(self.JOB_ID_RE.match(value), family.name)


class MetricTypeTests(unittest.TestCase):
    def test_declared_metric_types_are_prometheus_correct(self):
        types = {}
        for family in REGISTRY.collect():
            if family.name.startswith("jobqueue_"):
                types[family.name] = family.type

        self.assertEqual(types["jobqueue_jobs_processed"], "counter")
        self.assertEqual(types["jobqueue_job_processing_seconds"], "histogram")
        self.assertEqual(types["jobqueue_queue_wait_seconds"], "histogram")
        self.assertEqual(types["jobqueue_job_total_seconds"], "histogram")
        self.assertEqual(types["jobqueue_retry_delay_seconds"], "histogram")
        self.assertEqual(types["jobqueue_handler_errors"], "counter")
        self.assertEqual(types["jobqueue_reaped"], "counter")
        self.assertEqual(types["jobqueue_worker_inflight"], "gauge")
        self.assertEqual(types["jobqueue_api_requests"], "counter")
        self.assertEqual(types["jobqueue_api_request_duration_seconds"], "histogram")

    def test_cluster_collector_uses_gauge_and_counter_families(self):
        collector = metrics.QueueDepthCollector(FakeQueue())
        types = {family.name: family.type for family in collector.collect()}

        self.assertEqual(types["jobqueue_queue_depth"], "gauge")
        self.assertEqual(types["jobqueue_redis_up"], "gauge")
        self.assertEqual(types["jobqueue_jobs_enqueued"], "counter")
        self.assertEqual(types["jobqueue_failed_attempts"], "counter")
        self.assertEqual(types["jobqueue_dead_letters"], "counter")


if __name__ == "__main__":
    unittest.main()
