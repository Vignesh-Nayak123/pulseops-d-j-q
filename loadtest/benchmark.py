"""Benchmark harness for the distributed job queue.

Measures real numbers for a workload of N short jobs:
  - total drain time and throughput (jobs/sec)
  - enqueue-to-completion latency p50/p95/p99, read from the authoritative
    created_at / completed_at timestamps stored in each job hash
  - failed / dead / deduplicated counts from /stats deltas

Results are appended to reports/benchmark-results.json and printed.

Usage:
  python loadtest/benchmark.py --label baseline-1k --count 1000
  python loadtest/benchmark.py --label baseline-50k --count 50000
"""

import argparse
import json
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import redis as redislib
import requests

API = "http://localhost:8000"
REDIS_HOST, REDIS_PORT = "localhost", 6380


def percentile(sorted_vals, pct):
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, max(0, round((pct / 100.0) * (len(sorted_vals) - 1))))
    return sorted_vals[idx]


def enqueue_all(count, seconds, concurrency):
    """Enqueue `count` sleep jobs; return (ids, enqueue_wall_times, errors)."""
    session = requests.Session()
    ids, times_, errors = [], [], [0]
    lock = __import__("threading").Lock()

    def one(i):
        t0 = time.time()
        try:
            r = session.post(API + "/enqueue", json={
                "type": "sleep", "payload": {"seconds": seconds},
            }, timeout=15)
            r.raise_for_status()
            jid = r.json()["id"]
            with lock:
                ids.append(jid)
                times_.append(time.time() - t0)
        except Exception:
            with lock:
                errors[0] += 1

    started = time.time()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        for i, _ in enumerate(pool.map(one, range(count)), 1):
            if i % 5000 == 0:
                print("  enqueued %d/%d (%.1fs elapsed)" % (i, count, time.time() - started))
    return ids, times_, errors[0], started


def wait_for_completion(before_completed, expected, timeout):
    """Poll /stats until completed delta >= expected. Returns (drain_seconds, stats_after)."""
    t0 = time.time()
    last_report = 0.0
    while True:
        time.sleep(0.25)
        stats = requests.get(API + "/stats", timeout=10).json()
        done = stats["completed"] - before_completed
        now = time.time()
        if now - last_report >= 30:
            print("  %d/%d completed, inflight=%d, ready=%d (%.0fs)"
                  % (done, expected, stats["inflight"], stats["ready_total"], now - t0))
            last_report = now
        if done >= expected:
            return time.time() - t0, stats
        if now - t0 > timeout:
            return None, stats


def fetch_latencies(job_ids):
    """Read created_at/completed_at from Redis for every job; return list of ms latencies."""
    r = redislib.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True,
                       socket_timeout=30)
    lat = []
    BATCH = 1000
    for i in range(0, len(job_ids), BATCH):
        pipe = r.pipeline(transaction=False)
        for jid in job_ids[i:i + BATCH]:
            pipe.hmget("job:%s" % jid, "created_at", "completed_at")
        for created, completed in pipe.execute():
            if created and completed:
                lat.append((float(completed) - float(created)) * 1000.0)
    return lat


def main(argv=None):
    p = argparse.ArgumentParser(description="Queue benchmark")
    p.add_argument("--label", required=True)
    p.add_argument("--count", type=int, default=1000)
    p.add_argument("--seconds", type=float, default=0.01, help="handler sleep")
    p.add_argument("--concurrency", type=int, default=50)
    p.add_argument("--timeout", type=float, default=1800, help="drain timeout s")
    args = p.parse_args(argv)

    stats_before = requests.get(API + "/stats", timeout=10).json()
    print("[%s] enqueueing %d jobs (handler sleep %.3fs)..." % (args.label, args.count, args.seconds))

    ids, enq_lat, enq_errors, t_start = enqueue_all(args.count, args.seconds, args.concurrency)
    t_enqueued = time.time()
    enqueue_span = t_enqueued - t_start

    drain, stats_after = wait_for_completion(stats_before["completed"], len(ids), args.timeout)
    if drain is None:
        print("[%s] TIMEOUT: only %d/%d completed" % (args.label, stats_after["completed"] - stats_before["completed"], len(ids)))

    lat = fetch_latencies(ids)
    lat.sort()
    drain_total = (t_enqueued - t_start) + drain if drain else None
    result = {
        "label": args.label,
        "jobs_submitted": len(ids),
        "enqueue_errors": enq_errors,
        "enqueue_span_seconds": round(enqueue_span, 3),
        "drain_seconds": round(drain_total, 3) if drain_total else None,
        "throughput_jobs_per_sec": round(len(ids) / drain_total, 1) if drain_total else None,
        "latency_ms_p50": round(percentile(lat, 50), 1),
        "latency_ms_p95": round(percentile(lat, 95), 1),
        "latency_ms_p99": round(percentile(lat, 99), 1),
        "latency_ms_mean": round(statistics.mean(lat), 1) if lat else None,
        "latency_samples": len(lat),
        "completed_delta": stats_after["completed"] - stats_before["completed"],
        "failed_delta": stats_after["failed"] - stats_before["failed"],
        "dead_delta": stats_after["dead"] - stats_before["dead"],
        "deduplicated_delta": stats_after["deduplicated"] - stats_before["deduplicated"],
        "retry_metrics": {
            k: stats_after[k] - stats_before[k]
            for k in ("retried",) if k in stats_before
        },
    }
    print(json.dumps(result, indent=2))

    with open("reports/benchmark-results.json", "a") as f:
        f.write(json.dumps(result) + "\n")
    return 0 if drain else 1


if __name__ == "__main__":
    sys.exit(main())
