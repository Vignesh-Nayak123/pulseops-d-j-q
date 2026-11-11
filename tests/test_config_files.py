import json
import unittest
from pathlib import Path

try:
    import yaml
except ImportError:  # pragma: no cover - PyYAML is a test-time convenience
    yaml = None


ROOT = Path(__file__).resolve().parents[1]


class DashboardTests(unittest.TestCase):
    def setUp(self):
        self.dashboard = json.loads(
            (ROOT / "grafana" / "dashboards" / "jobqueue.json").read_text(encoding="utf-8")
        )

    def test_dashboard_identity_and_panels_are_valid(self):
        self.assertEqual(self.dashboard["uid"], "jobqueue-main")
        panels = self.dashboard["panels"]
        self.assertGreaterEqual(len(panels), 12)
        ids = [panel["id"] for panel in panels]
        self.assertEqual(len(ids), len(set(ids)))

        for panel in panels:
            self.assertEqual(panel["datasource"]["uid"], "prometheus")
            self.assertTrue(panel["targets"], panel["title"])
            for target in panel["targets"]:
                expr = target["expr"].strip()
                self.assertTrue(expr, panel["title"])
                self.assertNotIn("job_id=", expr)
                self.assertNotIn("idempotency_key=", expr)
                self.assertNotIn("last_error", expr)
                self.assertNotIn("payload", expr)

    def test_dashboard_covers_the_required_operational_views(self):
        titles = " ".join(panel["title"].lower() for panel in self.dashboard["panels"])
        for expected in (
            "queue depth",
            "throughput",
            "processing latency",
            "failure",
            "workers",
            "retry",
            "dead",
            "scheduled",
            "api",
            "handler errors",
            "promotion",
        ):
            self.assertIn(expected, titles)

    def test_worker_count_uses_target_health_not_the_worker_gauge(self):
        worker_panel = next(
            panel for panel in self.dashboard["panels"] if panel["title"] == "Workers and concurrency"
        )
        expressions = [target["expr"] for target in worker_panel["targets"]]
        self.assertIn('count(up{job="jobqueue-workers"} == 1)', expressions)
        self.assertNotIn("count(jobqueue_worker_inflight)", expressions)


@unittest.skipIf(yaml is None, "PyYAML is not installed")
class PrometheusConfigTests(unittest.TestCase):
    def setUp(self):
        self.config = yaml.safe_load(
            (ROOT / "prometheus" / "prometheus.yml").read_text(encoding="utf-8")
        )
        self.rules = yaml.safe_load(
            (ROOT / "prometheus" / "rules.yml").read_text(encoding="utf-8")
        )

    def test_scrape_config_covers_api_and_every_worker(self):
        self.assertIn("/etc/prometheus/rules.yml", self.config["rule_files"])
        jobs = {job["job_name"]: job for job in self.config["scrape_configs"]}

        self.assertEqual(
            jobs["jobqueue-api"]["static_configs"][0]["targets"],
            ["api:8000"],
        )
        worker_sd = jobs["jobqueue-workers"]["dns_sd_configs"][0]
        self.assertEqual(worker_sd["names"], ["worker"])
        self.assertEqual(worker_sd["port"], 9100)

    def test_recording_rules_cover_rates_percentiles_and_worker_health(self):
        rules = {
            rule["record"]: rule["expr"]
            for group in self.rules["groups"]
            for rule in group["rules"]
        }
        expected = {
            "jobqueue:jobs_processed:rate5m",
            "jobqueue:throughput:rate5m",
            "jobqueue:failure_ratio:rate5m",
            "jobqueue:queue_backlog:sum",
            "jobqueue:processing_latency:p95_5m",
            "jobqueue:queue_wait:p95_5m",
            "jobqueue:job_total_time:p95_5m",
            "jobqueue:workers_up:sum",
            "jobqueue:workers_inflight:sum",
            "jobqueue:reaped:rate5m",
            "jobqueue:handler_errors:rate5m",
            "jobqueue:api_error_ratio:rate5m",
            "jobqueue:api_latency:p95_5m",
            "jobqueue:promotions:rate5m",
            "jobqueue:idempotency_dedup:rate5m",
        }
        self.assertTrue(expected.issubset(rules))

        self.assertIn("sum by (job_type, outcome)", rules["jobqueue:jobs_processed:rate5m"])
        self.assertIn('count(up{job="jobqueue-workers"} == 1)', rules["jobqueue:workers_up:sum"])
        self.assertIn("histogram_quantile(0.95", rules["jobqueue:processing_latency:p95_5m"])

    def test_recording_rules_do_not_select_unbounded_labels(self):
        for group in self.rules["groups"]:
            for rule in group["rules"]:
                expr = rule["expr"]
                self.assertNotIn("job_id=", expr)
                self.assertNotIn("idempotency_key=", expr)
                self.assertNotIn("last_error", expr)
                self.assertNotIn("payload", expr)

    def test_worker_series_are_aggregated_across_replicas(self):
        rules = {
            rule["record"]: rule["expr"]
            for group in self.rules["groups"]
            for rule in group["rules"]
        }
        worker_metrics = (
            "jobqueue_jobs_processed_total",
            "jobqueue_job_processing_seconds_bucket",
            "jobqueue_queue_wait_seconds_bucket",
            "jobqueue_job_total_seconds_bucket",
            "jobqueue_retry_delay_seconds_bucket",
            "jobqueue_worker_inflight",
            "jobqueue_reaped_total",
            "jobqueue_handler_errors_total",
            "jobqueue_background_errors_total",
        )
        for name, expr in rules.items():
            if any(metric in expr for metric in worker_metrics):
                self.assertIn(
                    "sum",
                    expr,
                    "%s reads worker-local series without aggregating replicas" % name,
                )

    def test_cluster_counters_are_not_summed_across_instances(self):
        rules = {
            rule["record"]: rule["expr"]
            for group in self.rules["groups"]
            for rule in group["rules"]
        }
        for name in (
            "jobqueue:jobs_enqueued:rate5m",
            "jobqueue:jobs_completed:rate5m",
            "jobqueue:jobs_retried:rate5m",
            "jobqueue:dead_letters:rate5m",
            "jobqueue:promotions:rate5m",
            "jobqueue:idempotency_dedup:rate5m",
        ):
            self.assertNotIn(
                "sum(",
                rules[name],
                "%s must not sum a cluster-wide counter exposed once by the API" % name,
            )


@unittest.skipIf(yaml is None, "PyYAML is not installed")
class ComposeTests(unittest.TestCase):
    def test_prometheus_mounts_rules_and_workers_keep_metrics_port(self):
        compose = yaml.safe_load(
            (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
        )
        volumes = compose["services"]["prometheus"]["volumes"]
        self.assertTrue(any("rules.yml" in volume for volume in volumes))

        worker_env = compose["services"]["worker"]["environment"]
        self.assertEqual(worker_env["WORKER_METRICS_PORT"], "9100")


if __name__ == "__main__":
    unittest.main()
