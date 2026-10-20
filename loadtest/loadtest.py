"""Load generator: fire thousands of jobs at the API so you can watch the
queue drain in Grafana.

Run it from the host once the stack is up:

  python loadtest/loadtest.py --count 5000 --type sleep --seconds 0.2
  python loadtest/loadtest.py --count 2000 --type flaky --fail-rate 0.3
  python loadtest/loadtest.py --count 1000 --type sleep --priority high

It posts to /enqueue concurrently from a thread pool so the enqueue side is not
the bottleneck, then prints the resulting queue stats. Open Grafana at
http://localhost:3000 to watch ready depth spike and drain as the workers chew
through the backlog.
"""

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import requests


def build_job(args, index):
    """Construct one enqueue body based on the chosen job type."""
    if args.type == "sleep":
        payload = {"seconds": args.seconds}
    elif args.type == "flaky":
        payload = {"fail_rate": args.fail_rate}
    elif args.type == "email":
        payload = {"to": "user%d@example.com" % index, "subject": "load test %d" % index}
    else:
        payload = {}

    body = {"type": args.type, "payload": payload, "priority": args.priority}
    if args.delay_seconds:
        body["delay_seconds"] = args.delay_seconds
    return body


def enqueue_one(session, url, body):
    resp = session.post(url, json=body, timeout=10)
    resp.raise_for_status()
    return resp.json()


def main(argv=None):
    parser = argparse.ArgumentParser(description="Job queue load generator")
    parser.add_argument("--api", default="http://localhost:8000", help="API base URL")
    parser.add_argument("--count", type=int, default=5000, help="number of jobs to enqueue")
    parser.add_argument("--type", default="sleep", choices=["sleep", "flaky", "email"])
    parser.add_argument("--priority", default="default", choices=["high", "default", "low"])
    parser.add_argument("--seconds", type=float, default=0.2, help="sleep handler duration")
    parser.add_argument("--fail-rate", type=float, default=0.3, help="flaky handler failure probability")
    parser.add_argument("--delay-seconds", type=float, default=0.0, help="schedule jobs this far in the future")
    parser.add_argument("--concurrency", type=int, default=50, help="parallel enqueue requests")
    args = parser.parse_args(argv)

    enqueue_url = args.api.rstrip("/") + "/enqueue"
    print("enqueuing %d %r job(s) to %s with concurrency %d"
          % (args.count, args.type, enqueue_url, args.concurrency))

    session = requests.Session()
    start = time.time()
    submitted = 0
    errors = 0

    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = [
            pool.submit(enqueue_one, session, enqueue_url, build_job(args, i))
            for i in range(args.count)
        ]
        for fut in futures:
            try:
                fut.result()
                submitted += 1
            except Exception as exc:
                errors += 1
                if errors <= 5:  # show the first few, do not spam
                    print("enqueue error: %s" % exc, file=sys.stderr)

    elapsed = time.time() - start
    rate = submitted / elapsed if elapsed > 0 else 0
    print("submitted %d job(s) in %.2fs (%.0f jobs/sec), %d error(s)"
          % (submitted, elapsed, rate, errors))

    # Print a stats snapshot so you can see the backlog immediately.
    try:
        stats = session.get(args.api.rstrip("/") + "/stats", timeout=10).json()
        print("queue stats now: %s" % stats)
    except Exception as exc:
        print("could not fetch stats: %s" % exc, file=sys.stderr)

    print("open Grafana at http://localhost:3000 to watch the queue drain")


if __name__ == "__main__":
    main()
