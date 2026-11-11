import unittest
from unittest import mock

from jobqueue import config, queue


class FakePipeline:
    def __init__(self, results):
        self.results = results
        self.operations = []

    def zcard(self, key):
        self.operations.append(("zcard", key))
        return self

    def llen(self, key):
        self.operations.append(("llen", key))
        return self

    def zcount(self, key, minimum, maximum):
        self.operations.append(("zcount", key, minimum, maximum))
        return self

    def get(self, key):
        self.operations.append(("get", key))
        return self

    def execute(self):
        return self.results


class FakeStatsRedis:
    def __init__(self, results):
        self.results = results
        self.last_pipeline = None

    def pipeline(self):
        self.last_pipeline = FakePipeline(self.results)
        return self.last_pipeline


class FakeDedupeRedis:
    def __init__(self):
        self.incr_keys = []

    def set(self, *args, **kwargs):
        return False

    def get(self, key):
        return "existing-job-id"

    def incr(self, key):
        self.incr_keys.append(key)
        return 1


class StatsTests(unittest.TestCase):
    def test_stats_includes_new_cluster_counters_and_scheduled_due(self):
        results = [
            2, 3, 1,          # ready high/default/low
            4,                # scheduled
            2,                # inflight
            5,                # dead list length
            1,                # scheduled due now
            "100",            # enqueued
            "80",             # completed
            "12",             # failed attempts
            "7",              # handler retries
            "3",              # dead letters
            "9",              # promotions
            "4",              # idempotency dedupes
        ]
        redis = FakeStatsRedis(results)

        with mock.patch.object(queue, "get_client", return_value=redis):
            stats = queue.stats()

        self.assertEqual(stats["ready"], {"high": 2, "default": 3, "low": 1})
        self.assertEqual(stats["ready_total"], 6)
        self.assertEqual(stats["scheduled"], 4)
        self.assertEqual(stats["scheduled_due"], 1)
        self.assertEqual(stats["inflight"], 2)
        self.assertEqual(stats["dead"], 5)
        self.assertEqual(stats["enqueued"], 100)
        self.assertEqual(stats["completed"], 80)
        self.assertEqual(stats["failed"], 12)
        self.assertEqual(stats["retried"], 7)
        self.assertEqual(stats["dead_letters"], 3)
        self.assertEqual(stats["promoted"], 9)
        self.assertEqual(stats["deduplicated"], 4)

        operations = redis.last_pipeline.operations
        self.assertIn(
            ("zcount", queue.SCHEDULED_KEY, "-inf", mock.ANY),
            operations,
        )
        self.assertIn(("get", queue.STAT_DEDUPLICATED), operations)
        self.assertIn(("get", queue.STAT_PROMOTED), operations)
        self.assertIn(("get", queue.STAT_RETRIED), operations)
        self.assertIn(("get", queue.STAT_DEAD_LETTERS), operations)


class EnqueueDedupeTests(unittest.TestCase):
    def test_idempotent_dedupe_increments_cluster_counter(self):
        redis = FakeDedupeRedis()

        with mock.patch.object(queue, "get_client", return_value=redis):
            job_id, created = queue.enqueue(
                job_type="sleep",
                payload={"seconds": 0},
                idempotency_key="same-request",
            )

        self.assertEqual(job_id, "existing-job-id")
        self.assertFalse(created)
        self.assertEqual(redis.incr_keys, [queue.STAT_DEDUPLICATED])


class RetryDelayTests(unittest.TestCase):
    def test_matches_fail_lua_backoff_formula(self):
        self.assertEqual(queue.retry_delay_seconds(0), config.BACKOFF_BASE)
        self.assertEqual(queue.retry_delay_seconds(1), config.BACKOFF_BASE * 2)
        self.assertEqual(queue.retry_delay_seconds(2), config.BACKOFF_BASE * 4)

    def test_caps_at_backoff_max(self):
        self.assertEqual(queue.retry_delay_seconds(1000), config.BACKOFF_MAX)

    def test_invalid_attempts_are_treated_as_zero(self):
        self.assertEqual(queue.retry_delay_seconds(None), config.BACKOFF_BASE)
        self.assertEqual(queue.retry_delay_seconds("not-a-number"), config.BACKOFF_BASE)


class LuaKeyWiringTests(unittest.TestCase):
    def test_fail_passes_retry_and_dead_counters(self):
        calls = {}

        def fake_fail(**kwargs):
            calls.update(kwargs)
            return "retry"

        with mock.patch.object(queue, "_fail_script", fake_fail):
            self.assertEqual(queue.fail("job-id", "boom"), "retry")

        self.assertEqual(
            calls["keys"],
            [
                queue.INFLIGHT_KEY,
                queue.JOB_PREFIX + "job-id",
                queue.SCHEDULED_KEY,
                queue.DEAD_KEY,
                queue.STAT_FAILED,
                queue.STAT_RETRIED,
                queue.STAT_DEAD_LETTERS,
            ],
        )

    def test_reap_passes_dead_letter_counter(self):
        calls = {}

        def fake_reap(**kwargs):
            calls.update(kwargs)
            return (1, 2)

        with mock.patch.object(queue, "_reap_script", fake_reap):
            self.assertEqual(queue.reap(), (1, 2))

        self.assertEqual(
            calls["keys"],
            [
                queue.INFLIGHT_KEY,
                queue.SEQ_KEY,
                queue.DEAD_KEY,
                queue.STAT_FAILED,
                queue.STAT_DEAD_LETTERS,
            ],
        )

    def test_promote_passes_promoted_counter(self):
        calls = {}

        def fake_promote(**kwargs):
            calls.update(kwargs)
            return 3

        with mock.patch.object(queue, "_promote_script", fake_promote):
            self.assertEqual(queue.promote(), 3)

        self.assertEqual(
            calls["keys"],
            [queue.SCHEDULED_KEY, queue.SEQ_KEY, queue.STAT_PROMOTED],
        )


if __name__ == "__main__":
    unittest.main()
