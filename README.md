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

   Prometheus scrapes the API (cluster-wide queue depth and lifetime counters)
   and every worker replica (per-process throughput, latency, retries, reaper
   actions, and handler errors). Grafana reads Prometheus and draws the
   dashboard from the recording rules in prometheus/rules.yml.
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
| `jobqueue/metrics.py` | Bounded label helpers, API request metrics, and the cluster-wide Redis collector. |
| `jobqueue/worker_metrics.py` | Worker-only counters, gauges, and histograms. |
| `jobqueue/config.py` | All tunables, read from environment variables. |
| `prometheus/prometheus.yml` | Scrape config for the api and every worker replica. |
| `prometheus/rules.yml` | Recording rules for rates, percentiles, backlog, and worker health. |
| `grafana/` | Auto-provisioned datasource and dashboard. |
| `tests/` | Focused observability tests (metrics, labels, config, dashboard). |
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

### Generate a CSV report

The `generate_report` job writes deterministic sample order data to a CSV file.
Workers write to `reports/` and Compose bind-mounts it to the host, so reports
survive container recreation. Submit one from Windows PowerShell:

```powershell
$body = @{
  type = "generate_report"
  payload = @{ report_name = "orders"; rows = 250 }
} | ConvertTo-Json

$job = Invoke-RestMethod -Method Post -Uri http://localhost:8000/enqueue `
  -ContentType "application/json" -Body $body
$job.id
```

Check its status and result using the returned id:

```powershell
Invoke-RestMethod "http://localhost:8000/jobs/$($job.id)" | ConvertTo-Json -Depth 5
```

While the job is queued the status is `ready`, `scheduled`, or `running`. Once it
is `completed`, the job record includes a `result` object like:

```json
{
  "filename": "orders-<job-id>.csv",
  "rows": 250,
  "path": "/app/reports/orders-<job-id>.csv"
}
```

The same file is available on the host under `reports/`:

```powershell
Get-ChildItem .\reports
Get-Content ".\reports\orders-<job-id>.csv" -TotalCount 5
Copy-Item ".\reports\orders-<job-id>.csv" "$HOME\Downloads\"
```

`report_name` is optional (default `orders`) and is sanitized into a safe
filename. `rows` is optional (default `100`) and must be an integer from 1 to
100000; invalid values fail the job and go through the normal retry and
dead-letter handling. If the stack is already running an older image, rebuild
it first with `docker compose up --build`.

### Text statistics

`text_stats` counts characters, words, and newline-separated lines. Empty text
has 0 lines; otherwise lines = number of `\n` characters + 1.
`characters_without_spaces` removes literal space characters only. The supplied
text is not included in the result or routine worker logs.

```powershell
$body = @{
  type = "text_stats"
  payload = @{ text = "Hello world`nSecond line" }
} | ConvertTo-Json

$job = Invoke-RestMethod -Method Post -Uri http://localhost:8000/enqueue `
  -ContentType "application/json" -Body $body

Start-Sleep -Seconds 1
Invoke-RestMethod "http://localhost:8000/jobs/$($job.id)" | ConvertTo-Json -Depth 5
```

The completed job contains:

```json
{
  "characters": 23,
  "words": 4,
  "lines": 2,
  "characters_without_spaces": 21
}
```

### Send an email through SMTP

`send_email` uses the worker's SMTP configuration. Set the `SMTP_*` variables in
`.env` (Docker Compose reads it automatically) or export them in your shell for a
non-Docker run. Compose defaults to `host.docker.internal:1025` so workers can
reach Mailpit on the Docker host; a direct non-Docker run defaults to
`localhost:1025`. Neither delivers to real mailboxes. Restart the workers after
changing SMTP settings.

```powershell
$body = @{
  type = "send_email"
  payload = @{
    to = "recipient@example.com"
    subject = "Hello from the queue"
    body = "This message was sent by the job queue."
  }
} | ConvertTo-Json

$job = Invoke-RestMethod -Method Post -Uri http://localhost:8000/enqueue `
  -ContentType "application/json" -Body $body

Invoke-RestMethod "http://localhost:8000/jobs/$($job.id)" | ConvertTo-Json -Depth 5
```

A successful job records `"result": { "sent_to": "recipient@example.com" }`.
Delivery failures appear as `last_error` and follow the normal retry/backoff and
dead-letter behavior. The handler never returns or logs the message subject or
body.

Important: delivery is at-least-once. If the SMTP server accepts the email but
the worker dies before recording success, the retry can send a duplicate. This
handler does not implement provider-side idempotency. For production email, use
a provider API with an idempotency key or another deduplication strategy where
available.

#### Local SMTP test server

The Compose workers connect to Mailpit on the Docker host at
`host.docker.internal:1025` with STARTTLS disabled. Ensure the project's `.env`
contains these exact settings:

```dotenv
SMTP_HOST=host.docker.internal
SMTP_PORT=1025
SMTP_USERNAME=
SMTP_PASSWORD=
SMTP_FROM=no-reply@localhost
SMTP_STARTTLS=false
SMTP_TIMEOUT=10
```

Start Mailpit in the background. Binding SMTP to `0.0.0.0` and disabling reverse
DNS avoids delays when the worker connects from Docker Desktop:

```powershell
docker run --rm -d --name mailpit `
  -p 1025:1025 -p 8025:8025 `
  axllent/mailpit --smtp 0.0.0.0:1025 --smtp-disable-rdns
```

Recreate every worker so all replicas receive the new SMTP environment:

```powershell
docker compose up -d --force-recreate worker
```

Submit a new email job:

```powershell
$body = @{
  type = "send_email"
  payload = @{
    to = "recipient@example.com"
    subject = "Hello from the queue"
    body = "This message was sent by the job queue."
  }
} | ConvertTo-Json

$job = Invoke-RestMethod -Method Post -Uri http://localhost:8000/enqueue `
  -ContentType "application/json" -Body $body
$job.id
```

Wait for the job to finish and inspect its status:

```powershell
do {
  $state = Invoke-RestMethod "http://localhost:8000/jobs/$($job.id)"
  Start-Sleep -Milliseconds 500
} until ($state.status -in @("completed", "dead"))
$state | ConvertTo-Json -Depth 5
```

Open Mailpit in your browser:

```powershell
Start-Process "http://localhost:8025"
```

The captured message appears in Mailpit's inbox. If a job was already
dead-lettered before Mailpit was running, it will not be retried automatically:
submit a new email job after Mailpit and the workers are up.

#### Real SMTP provider

Use the SMTP hostname, port, username, and password supplied by your provider.
Commonly that means port 587 with STARTTLS enabled, but follow the provider's
documentation. Use an app-specific password where available and never commit
`.env` or real credentials.

```dotenv
SMTP_HOST=smtp.your-provider.example
SMTP_PORT=587
SMTP_USERNAME=your-provider-username
SMTP_PASSWORD=your-app-specific-password
SMTP_FROM=verified-sender@example.com
SMTP_STARTTLS=true
SMTP_TIMEOUT=10
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

Prometheus scrapes the API at `api:8000/metrics` and every worker replica at
`worker:9100/metrics`. The API exposes cluster-wide Redis-derived queue state and
lifetime counters; workers expose process-local counters and histograms. Docker's
DNS service discovery finds every worker replica, so scaling the worker pool does
not require a scrape-config change. The dashboard and recording rules aggregate
across all worker instances.

Queue-depth gauges are read from Redis once per Prometheus scrape (5 seconds in
the provided config), so they refresh even when no jobs are moving. There is no
background polling loop and no per-job Redis polling added for metrics.

### Metric catalog

#### Cluster-wide metrics (API `/metrics`)

These are exposed once for the whole Redis-backed cluster by
`QueueDepthCollector`. They are accurate across workers because Redis is the
shared source of truth.

| Metric | Type | Labels | Meaning | Scope |
| --- | --- | --- | --- | --- |
| `jobqueue_queue_depth` | Gauge | `state` | Current jobs in a state: `ready:<priority>`, `ready_total`, `scheduled`, `scheduled_due`, `inflight`, `dead`. `scheduled_due` counts scheduled jobs whose run-at time has arrived and are awaiting promotion. `dead` is the dead-letter list length. | Cluster-wide snapshot, refreshed every scrape. |
| `jobqueue_redis_up` | Gauge | none | `1` when the API could read queue state from Redis during this scrape, `0` otherwise. | API process. |
| `jobqueue_jobs_enqueued_total` | Counter | none | New jobs created by enqueue calls. Idempotent dedupe returns are excluded. | Cluster-wide lifetime. |
| `jobqueue_jobs_completed_total` | Counter | none | Jobs acknowledged as completed. | Cluster-wide lifetime. |
| `jobqueue_failed_attempts_total` | Counter | none | Failed delivery attempts: handler failures and visibility timeouts, including retries. This counts attempts, not unique jobs. | Cluster-wide lifetime. |
| `jobqueue_jobs_retried_total` | Counter | none | Handler failures scheduled for a backoff retry. Visibility-timeout requeues are separate. | Cluster-wide lifetime. |
| `jobqueue_dead_letters_total` | Counter | none | Jobs moved to the dead-letter list after exhausting handler attempts or visibility-timeout attempts. | Cluster-wide lifetime. |
| `jobqueue_jobs_promoted_total` | Counter | none | Scheduled or backed-off jobs moved into a ready queue by a promoter. | Cluster-wide lifetime. |
| `jobqueue_idempotency_dedup_total` | Counter | none | Enqueue requests deduplicated by an `idempotency_key`. | Cluster-wide lifetime. |

Completed and failed jobs are not a current queue state in this design, so they
are represented by lifetime counters rather than queue-depth gauges. Only ready
queues are priority-indexed in Redis; scheduled, inflight, and dead-letter depth
are cluster-wide totals.

#### Worker metrics (each worker `:9100/metrics`)

These series are process-local. Prometheus scrapes every worker replica; the
dashboard sums or rates across them. A worker restart resets its counters, which
`rate()` and `increase()` handle as normal counter resets.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `jobqueue_jobs_processed_total` | Counter | `job_type`, `outcome`, `priority` | Concluded delivery attempts. `outcome` is `completed`, `retry`, `dead`, or `lost`. A job can appear as `retry` and later as `completed` or `dead`; this is not a final-job counter. |
| `jobqueue_job_processing_seconds` | Histogram | `job_type`, `outcome` | Handler execution time per delivery attempt. Failures are observed too. |
| `jobqueue_queue_wait_seconds` | Histogram | `job_type`, `priority` | Enqueue-to-start wall time. For delayed jobs and retries this includes intentional delay or backoff. |
| `jobqueue_job_total_seconds` | Histogram | `job_type`, `priority` | Enqueue-to-successful-completion wall time. Observed only when the ack succeeds. |
| `jobqueue_retry_delay_seconds` | Histogram | `job_type` | Backoff delay chosen for a handler-failure retry. Visibility-timeout requeues are immediate and counted by `jobqueue_reaped_total`. |
| `jobqueue_handler_errors_total` | Counter | `job_type`, `error_type` | Handler exceptions. `error_type` is the exception class name, never the exception message or payload. |
| `jobqueue_reaped_total` | Counter | `action` | Visibility-timeout actions: `requeued` or `dead`. The Lua script guarantees each expired job is handled once, so summing across workers is accurate. |
| `jobqueue_background_errors_total` | Counter | `loop` | Errors raised by a worker's `reaper` or `promoter` loop. |
| `jobqueue_worker_inflight` | Gauge | none | Jobs currently executing in this worker process. With the one-job-at-a-time model this is 0 or 1; sum across workers for live concurrency. |

Prometheus also exposes its standard `up` series for each scrape target and
`process_*`/`python_gc_*` process metrics from the Prometheus client.

#### API request metrics (API `/metrics`)

These are process-local to the API process. The current Compose setup runs one
API worker; if you scale API replicas, sum across them.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `jobqueue_api_requests_total` | Counter | `method`, `path`, `status` | HTTP requests by method, Flask route pattern, and status code. `path` is the route pattern such as `/jobs/<job_id>`, never the concrete job id. `/metrics` scrapes are excluded. |
| `jobqueue_api_request_duration_seconds` | Histogram | `method`, `path` | API request latency by method and route pattern. `/metrics` scrapes are excluded. |

### Label safety

Every label is bounded:

- `job_type` is a registered handler type or `unregistered`.
- `priority` is one of the configured priorities or `unknown`.
- `error_type` is the Python exception class name or `exception`; exception text
  is never used.
- API `path` is the Flask route pattern or `unmatched`.
- API `method`, `status`, and queue `state` come from small fixed sets.

Job IDs, idempotency keys, payload values, email addresses, credentials, and
exception messages are never used as label values. The SMTP, report, and text
handlers are observed through the generic `job_type` labels; no payload-derived
metrics are emitted.

### Scope, duplication, and counter resets

- **Cluster-wide counters** come from Redis and survive worker restarts. They
  reset only if Redis data is lost. `jobqueue_queue_depth` is a live gauge, not a
  counter.
- **Worker and API counters** are process-local and reset when that process
  restarts. Use `rate()` or `increase()`; Prometheus handles resets. Sum the
  per-instance series for cluster throughput.
- `jobqueue_jobs_processed_total{outcome="completed"}` and
  `jobqueue_jobs_completed_total` describe the same acks at different scopes
  (worker dimension vs. cluster lifetime). Do not add them together. The same
  applies to worker `outcome="retry"` versus `jobqueue_jobs_retried_total`, and
  to worker `outcome="dead"` versus `jobqueue_dead_letters_total`.
- `jobqueue_failed_attempts_total` counts failed attempts, not unique jobs. A job
  retried three times can contribute three failed attempts plus one completion.
- `jobqueue_dead_letters_total` includes dead letters produced by the reaper
  after visibility timeouts, not only worker-observed `outcome="dead"` events.
- `jobqueue_jobs_processed_total{outcome="retry"}` counts handler-failure
  retries. Reaper requeues are visibility timeouts and appear in
  `jobqueue_reaped_total{action="requeued"}` and
  `jobqueue_failed_attempts_total`.
- `jobqueue_queue_wait_seconds` includes intentional delay and backoff;
  `jobqueue_job_processing_seconds` is handler execution only.
- `jobqueue_jobs_enqueued_total` counts newly created jobs. An idempotent
  submission that returns an existing job increments
  `jobqueue_idempotency_dedup_total` instead.

### Upgrading from earlier metric names

The older `jobqueue_jobs_total{kind="enqueued|completed|failed"}` series has been
replaced by `jobqueue_jobs_enqueued_total`, `jobqueue_jobs_completed_total`, and
`jobqueue_failed_attempts_total`. That gives each counter a stable name and an
accurate help string. Update any saved queries that still select
`jobqueue_jobs_total`.

Two worker series gained labels without changing their names:
`jobqueue_jobs_processed_total` now has a `priority` label, and
`jobqueue_job_processing_seconds` now has an `outcome` label. Existing
aggregations that sum across workers keep working; queries that grouped only by
`job_type` may now show one series per priority or outcome.

### Retry versus final failure

A retry is not a final failure. A handler failure produces a `retry` outcome and
a backoff delay while attempts remain; once attempts are exhausted it produces a
`dead` outcome. A visibility timeout produces a reaper `requeued` action while
attempts remain and a reaper `dead` action once exhausted. Final failures are
therefore:

```promql
sum(increase(jobqueue_jobs_processed_total{outcome="dead"}[1h]))
+
sum(increase(jobqueue_reaped_total{action="dead"}[1h]))
```

For an all-in one cluster-wide counter, use:

```promql
increase(jobqueue_dead_letters_total[1h])
```

### PromQL examples

```promql
# Throughput across all workers.
sum(rate(jobqueue_jobs_processed_total[5m]))

# Throughput by outcome and job type.
sum by (job_type, outcome) (rate(jobqueue_jobs_processed_total[5m]))

# Failure ratio (retry + dead + lost as a share of concluded attempts).
sum(rate(jobqueue_jobs_processed_total{outcome=~"retry|dead|lost"}[5m]))
  / clamp_min(sum(rate(jobqueue_jobs_processed_total[5m])), 0.0001)

# Handler retry rate and final dead-letter rate.
sum(rate(jobqueue_jobs_processed_total{outcome="retry"}[5m]))
rate(jobqueue_dead_letters_total[5m])

# Processing latency percentiles.
histogram_quantile(0.95, sum by (le) (rate(jobqueue_job_processing_seconds_bucket[5m])))

# Queue wait and total job time.
histogram_quantile(0.95, sum by (le) (rate(jobqueue_queue_wait_seconds_bucket[5m])))
histogram_quantile(0.95, sum by (le) (rate(jobqueue_job_total_seconds_bucket[5m])))

# Ready + scheduled backlog and current dead-letter depth.
sum(jobqueue_queue_depth{state=~"ready_total|scheduled"})
jobqueue_queue_depth{state="dead"}

# Workers up, jobs in flight, and scheduled jobs due now.
count(up{job="jobqueue-workers"} == 1)
sum(jobqueue_worker_inflight)
jobqueue_queue_depth{state="scheduled_due"}

# Reaper actions, promotions, and idempotency deduplication.
sum by (action) (rate(jobqueue_reaped_total[5m]))
rate(jobqueue_jobs_promoted_total[5m])
rate(jobqueue_idempotency_dedup_total[5m])

# Handler errors by type and exception class.
sum by (job_type, error_type) (rate(jobqueue_handler_errors_total[5m]))

# API request rate, 5xx ratio, and p95 latency.
sum by (status) (rate(jobqueue_api_requests_total[5m]))
sum(rate(jobqueue_api_requests_total{status=~"5.."}[5m]))
  / clamp_min(sum(rate(jobqueue_api_requests_total[5m])), 0.0001)
histogram_quantile(0.95, sum by (le) (rate(jobqueue_api_request_duration_seconds_bucket[5m])))
```

### Recording rules and dashboard

`prometheus/rules.yml` contains ready-to-use recording rules for throughput,
failure ratio, retries, dead letters, backlog, processing/queue-wait/total-time
percentiles, reaper actions, promotions, deduplication, handler errors, worker
health, and API request metrics. Prometheus loads the file through
`prometheus/prometheus.yml`.

Open Grafana at http://localhost:3000 (anonymous access is enabled) and choose
the **Job Queue** dashboard, or go directly to
http://localhost:3000/d/jobqueue-main. Prometheus is at
http://localhost:9090 for raw queries and rule inspection.

The dashboard includes:

- Queue depth by state, ready depth by priority, and scheduled backlog.
- Throughput by outcome, failure ratio, and processing latency percentiles.
- Queue wait and total job time percentiles.
- Handler retries, reaper actions, dead-letter rates, and background errors.
- Worker count/availability (from `up`), in-flight jobs, and scheduled due.
- Enqueued/completed/promoted/deduplicated rates and cluster lifetime counters.
- API request rate by status, 5xx ratio, and API latency.
- Handler errors by job type and exception class.

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
| `REPORTS_DIR` | `reports/` | Directory where generated CSV reports are written. |
| `SMTP_HOST` | `localhost` | SMTP server for `send_email`. |
| `SMTP_PORT` | `1025` | SMTP server port. |
| `SMTP_USERNAME` | empty | SMTP username; blank skips authentication. |
| `SMTP_PASSWORD` | empty | SMTP password; never commit it. |
| `SMTP_FROM` | `no-reply@localhost` | Sender address used for outgoing mail. |
| `SMTP_STARTTLS` | `false` | Enable STARTTLS when `true`. |
| `SMTP_TIMEOUT` | `10` | SMTP connection timeout in seconds. |

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
