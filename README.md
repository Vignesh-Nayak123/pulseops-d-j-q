# Distributed Job Queue

A horizontally scalable job queue backed by Redis, with a Flask REST API, a pool
of competing worker processes, at-least-once delivery, retries with exponential
backoff, dead-letter handling, priorities, delayed and scheduled jobs,
idempotency, and full Prometheus plus Grafana observability. The entire system
comes up with a single `docker compose up`.

## What it is

You submit a job over HTTP. The API writes it to Redis and returns immediately.
A fleet of worker processes pulls jobs from Redis and runs them in the
background. Workers compete for jobs safely: no two workers ever run the same
job at the same time under normal operation, and if a worker crashes mid-job the
work is automatically retried rather than lost.

This is the same pattern behind Sidekiq, Celery, Resque, and managed services
like Amazon SQS. This project builds the core of one from scratch to show how the
hard parts actually work.

## The real-world problem it solves

Web requests should be fast. A lot of useful work is not: sending email,
transcoding video, generating reports, calling slow third-party APIs, charging a
card. If you do that work inside the request, the user waits, and one slow
dependency can pile up requests until the whole service falls over.

A job queue decouples "someone asked for this work" from "the work runs." The
API accepts the request, records the job, and responds in milliseconds. Workers
do the slow part out of band. This buys you several things at once:

- Responsiveness. The user is not blocked on slow work.
- Absorbing spikes. A traffic burst becomes a queue backlog that drains over
  time instead of a pile of timed-out requests.
- Horizontal scale. Need more throughput? Run more workers.
- Resilience. A worker crash does not lose the job. It gets retried.
- Smoothing. Expensive or rate-limited downstreams see a controlled rate of
  work instead of a thundering herd.

## Architecture

```
                           HTTP
   client / loadtest  ------------->  +-------------------+
                                      |   API (Flask)     |
                                      |  POST /enqueue    |
                                      |  GET  /jobs/<id>  |
                                      |  GET  /stats      |
                                      |  GET  /metrics    |
                                      +---------+---------+
                                                |
                                  enqueue / read state
                                                |
                                                v
   +--------------------------------------------------------------+
   |                           Redis                              |
   |                                                              |
   |   queue:ready:high    ZSET  ready jobs, scored by sequence   |
   |   queue:ready:default ZSET  (FIFO within a priority)         |
   |   queue:ready:low     ZSET                                   |
   |   queue:scheduled     ZSET  delayed jobs + backoff retries,  |
   |                             scored by run-at timestamp       |
   |   queue:inflight      ZSET  claimed jobs, scored by the      |
   |                             visibility deadline              |
   |   queue:dead          LIST  dead-letter queue                |
   |   job:<id>            HASH  the full job record              |
   |   idempotency:<key>   STR   dedupe marker (SET NX)           |
   +--------------------------------------------------------------+
          ^                  ^                 ^             ^
          | claim (Lua)      | ack / fail (Lua)|             |
          |                  |                 | promote     | reap
   +------+------+    +-------+------+          | (Lua)       | (Lua)
   |  worker 1   |    |  worker 2    |   ...    |             |
   |  claim->run |    |  claim->run  |     each worker also runs:
   |  ->ack/fail |    |  ->ack/fail  |       promoter loop (scheduled -> ready)
   +-------------+    +--------------+       reaper loop   (inflight -> ready/dead)

   Prometheus scrapes the API (queue depth) and every worker (throughput,
   latency, failures). Grafana reads Prometheus and draws the dashboard.
```

Data flow of one job:

1. `POST /enqueue` writes a `job:<id>` hash and pushes the id onto a ready queue
   (or the scheduled set if it is delayed).
2. A worker atomically claims the id: it moves from a ready queue into
   `queue:inflight` with a visibility deadline, in one Lua script.
3. The worker runs the handler for the job's type.
4. On success it acks: the id is removed from inflight and the job is marked
   completed. On failure it either schedules a backoff retry or dead-letters the
   job once it is out of attempts.
5. If the worker dies before step 4, the reaper sees the expired inflight entry
   and makes the job visible again (or dead-letters it if it has exhausted its
   attempts).

## Components

| Path | Role |
| --- | --- |
| `jobqueue/api.py` | Flask REST API. |
| `jobqueue/worker.py` | Worker loop plus the reaper and promoter threads. |
| `jobqueue/queue.py` | Core queue operations and the Redis key layout. |
| `jobqueue/lua/*.lua` | The atomic operations: claim, ack, fail, promote, reap. |
| `jobqueue/jobs.py` | Handler registry and the sample handlers. |
| `jobqueue/metrics.py` | Prometheus collectors shared by api and workers. |
| `jobqueue/config.py` | All tunables, read from environment variables. |
| `prometheus/prometheus.yml` | Scrape config for the api and the workers. |
| `grafana/` | Auto-provisioned datasource and dashboard. |
| `loadtest/loadtest.py` | Load generator. |

## Design decisions

### Atomic claim, and why it is race-free

This is the heart of the system. When a worker claims a job it must do two things
together: take the job off a ready queue, and record that it now owns it. If
those two steps are separate, two workers can both read the same job id before
either marks it taken, and the job runs twice at the same time.

The claim is a single Lua script (`jobqueue/lua/claim.lua`) run with `EVAL`.
Redis executes a Lua script as one indivisible unit. The server runs commands on
a single thread, and while a script runs no other client's commands interleave
with it. So the whole sequence inside the script (scan the ready queues highest
priority first, pop the oldest id with `ZPOPMIN`, add it to the inflight set with
a deadline, flip the job's status to running) happens with nothing else able to
observe a half-finished state. There is no window between "I saw this id" and "I
took this id" for a second worker to slip into.

Contrast the naive approach in application code: read a job id, then in a second
round trip remove it. Two workers can both complete the read before either does
the remove, and both proceed to run the job. Folding the read and the take into
one server-side script closes that gap completely. The same reasoning is why ack,
fail, promote, and reap are also Lua scripts: each one reads some state, makes a
decision, and writes a result, and that read-decide-write must be atomic against
every other worker and against the reaper.

### Visibility timeout and at-least-once delivery

When a worker claims a job, the job id goes into `queue:inflight` scored by a
deadline equal to `now + VISIBILITY_TIMEOUT`. For that window the job is invisible
to other workers, because claim only ever pulls from the ready queues, never from
inflight. A healthy worker removes the id when it acks or fails.

If the worker crashes, hangs, or is killed, it never removes the id. The deadline
passes, and the reaper (`jobqueue/lua/reap.lua`, run on a loop by every worker)
finds the expired entry and makes the job visible again so another worker can run
it. That is why delivery is at-least-once and not at-most-once: faced with a
silent worker death, the system chooses to run the job again rather than risk
losing it. Handlers should therefore be idempotent, or use the idempotency key.

The visibility timeout must be set comfortably larger than your slowest expected
job. If it is too short, the reaper will requeue jobs that are still happily
running, and they will execute more than once. This was verified directly: a job
that sleeps 35 seconds under a 30 second timeout gets requeued by the reaper
mid-run, a second worker also runs it, and the original worker's late ack is
rejected because it no longer owns the lease.

Running the reaper inside every worker rather than in one dedicated process means
there is no single point of failure for recovery. It is safe to run many reapers
at once because the requeue is atomic: the script removes the id from inflight
with `ZREM` and only the one call that actually removed it acts on the job.

### Retry with exponential backoff, and the poison-job guard

A job carries an `attempts` counter. It counts concluded failed deliveries, and
it is incremented in exactly one place per outcome:

- `fail.lua` increments it when a handler raises.
- `reap.lua` increments it when a delivery times out (a crashed or stuck worker).

It is deliberately not incremented at claim time, so a crashed job is not
double-counted (once by the reaper, once when it is re-claimed).

On a handler failure, if `attempts` is still below `MAX_ATTEMPTS` the job is
pushed into `queue:scheduled` at `now + BACKOFF_BASE * 2^(attempts - 1)`, capped
at `BACKOFF_MAX`. So the first retry waits `BACKOFF_BASE`, the next twice that,
then twice that again. Backoff gives a struggling downstream room to recover
instead of being hammered by instant retries. The promoter loop moves the job
back to a ready queue once its scheduled time arrives.

Once `attempts` reaches `MAX_ATTEMPTS` the job is moved to the dead-letter list
`queue:dead` for inspection instead of being retried forever.

Crucially, timeouts and handler failures share the same counter and the same
limit. A poison job that repeatedly crashes its worker (and therefore keeps
timing out) is requeued by the reaper only until it has burned `MAX_ATTEMPTS`
deliveries, then the reaper itself dead-letters it. A job cannot loop forever no
matter whether it fails by raising or by killing its worker. This was verified:
an always-failing job recorded exactly 5 failed attempts with the expected
2, 4, 8, 16 second gaps, then landed in the dead-letter queue at `attempts = 5`.

### Idempotency

Submitting the same logical job twice (a retry from a flaky client, a
double-clicked button, an at-least-once upstream) should not run the work twice.
On enqueue you may pass an `idempotency_key`. The API reserves it with
`SET idempotency:<key> <job_id> NX`. `NX` means set only if the key does not
already exist, and Redis makes that check-and-set atomic. The first caller wins
and gets a freshly created job. A later caller with the same key finds the key
already set, gets back the original job id, and nothing new is enqueued. The
response distinguishes the two cases: HTTP 201 with `"created": true` for a new
job, HTTP 200 with `"created": false` for a deduplicated one.

This dedupes at the front door, before a job ever enters a queue, which is
cheaper and simpler than trying to detect duplicates among already-running jobs.

### Priorities and scheduling

There is one ready queue per priority (`high`, `default`, `low`). The claim
script scans them highest first, so a single high-priority job is taken before
any default or low job. Within a priority, jobs are ordered by an increasing
sequence number used as the sorted-set score, which gives first-in-first-out
order.

Delayed and scheduled jobs go into `queue:scheduled`, a sorted set scored by the
run-at timestamp. The promoter loop moves due jobs into their ready queue when
their time arrives. The same mechanism carries backoff retries, since a retry is
just a job scheduled for a little while in the future.

### One job at a time per worker

Each worker process runs exactly one job at a time. You scale throughput by
running more worker containers, not by threading inside a worker. This keeps the
claim and ack story simple to reason about and makes the safety guarantees easy
to demonstrate. The reaper and promoter run as lightweight background threads in
each worker and do not execute jobs themselves.

## Running it

Requirements: Docker and Docker Compose.

```
docker compose up --build
```

That brings up Redis, the API, three workers, Prometheus, and Grafana. Useful
endpoints once it is up:

- API: http://localhost:8000
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (anonymous access is enabled, dashboard
  "Job Queue" is provisioned automatically)

Scale the worker pool up or down at any time:

```
docker compose up --scale worker=10
```

### Try it by hand

Enqueue a simulated-work job:

```
curl -X POST http://localhost:8000/enqueue \
  -H "Content-Type: application/json" \
  -d '{"type":"sleep","payload":{"seconds":2}}'
```

Check its status (use the id from the response above):

```
curl http://localhost:8000/jobs/<id>
```

See queue stats:

```
curl http://localhost:8000/stats
```

A high-priority job, a delayed job, and an idempotent job:

```
curl -X POST http://localhost:8000/enqueue -H "Content-Type: application/json" \
  -d '{"type":"email","payload":{"to":"a@b.com"},"priority":"high"}'

curl -X POST http://localhost:8000/enqueue -H "Content-Type: application/json" \
  -d '{"type":"sleep","payload":{"seconds":1},"delay_seconds":30}'

curl -X POST http://localhost:8000/enqueue -H "Content-Type: application/json" \
  -d '{"type":"email","payload":{"to":"a@b.com"},"idempotency_key":"welcome-a@b.com"}'
```

Watch retries and the dead-letter queue with an always-failing job:

```
curl -X POST http://localhost:8000/enqueue -H "Content-Type: application/json" \
  -d '{"type":"flaky","payload":{"fail_rate":1.0}}'
```

## Running the load test

With the stack up, fire thousands of jobs and watch the queue drain in Grafana.
Run it from the host (it needs the `requests` library, or run it inside the
container as shown below):

```
python loadtest/loadtest.py --count 5000 --type sleep --seconds 0.2
```

Or without installing anything locally, run it inside the already-built image:

```
docker compose run --rm api python loadtest/loadtest.py --count 5000 --type sleep --seconds 0.2
```

Open Grafana at http://localhost:3000 and watch the "Queue depth by state" panel
spike and then drain as the workers process the backlog, while throughput,
latency, and failure-rate panels move in response. Scale workers mid-drain to see
the backlog clear faster:

```
docker compose up --scale worker=10
```

Other useful runs:

```
# Exercise retries, backoff, and the dead-letter queue.
python loadtest/loadtest.py --count 2000 --type flaky --fail-rate 0.3

# Push high-priority work so it jumps ahead of a default backlog.
python loadtest/loadtest.py --count 1000 --type sleep --priority high
```

## Metrics

The API exposes cluster-wide queue depth at `/metrics`. Each worker exposes its
own throughput, latency, and failure metrics on port 9100. Prometheus discovers
all worker replicas automatically through Docker's DNS, so the metrics keep
working no matter how far you scale the worker pool. Key series:

- `jobqueue_queue_depth{state=...}` ready (total and per priority), scheduled,
  inflight, and dead-letter depth.
- `jobqueue_jobs_processed_total{job_type,outcome}` counter behind throughput and
  failure rate. `rate()` of it is jobs per second.
- `jobqueue_job_processing_seconds` handler execution time histogram, used for
  p50, p95, and p99 latency.
- `jobqueue_reaped_total{action}` jobs requeued or dead-lettered after a
  visibility timeout.
- `jobqueue_worker_inflight` jobs running right now, summed across workers to
  show live concurrency.

## Configuration

Everything tunable lives in `jobqueue/config.py` and is set by environment
variable. The important ones, with their defaults:

| Variable | Default | Meaning |
| --- | --- | --- |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection. |
| `VISIBILITY_TIMEOUT` | `30` | Seconds a claimed job stays invisible before the reaper requeues it. |
| `MAX_ATTEMPTS` | `5` | Failed deliveries (handler failures plus timeouts) before dead-lettering. |
| `BACKOFF_BASE` | `2` | Base seconds for exponential backoff. |
| `BACKOFF_MAX` | `300` | Cap on a single backoff delay. |
| `REAP_INTERVAL` | `1` | Seconds between reaper scans. |
| `PROMOTE_INTERVAL` | `1` | Seconds between promoter scans. |
| `RESULT_TTL` | `3600` | Seconds a finished job record stays queryable. |

## Notes and trade-offs

- Delivery is at-least-once by design. Handlers should be idempotent or use the
  idempotency key. At-most-once would mean dropping jobs on a worker crash, which
  is usually worse.
- The reaper and promoter run in every worker for resilience. The atomic scripts
  make concurrent reaping and promoting safe.
- This is a portfolio project focused on the queue mechanics. For production you
  would also want authentication on the API, Redis persistence and failover tuned
  for your durability needs, structured logging, and alerting rules on the
  metrics.
