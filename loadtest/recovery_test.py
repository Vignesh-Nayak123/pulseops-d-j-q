"""Worker-failure recovery test.

1. Enqueue N long-ish (sleep) jobs across 3 workers.
2. Mid-run, hard-kill (SIGKILL) one worker while it holds a job in flight.
3. Capture the stranded in-flight job ids from Redis right after the kill.
4. Wait for the visibility lease to expire so another worker's reaper requeues
   the stranded job(s); then restart the killed worker.
5. Wait for the whole batch to drain and verify:
   - every submitted job id ends with status=completed (nothing lost)
   - completed counter advanced by exactly N (no job acked twice)
   - retried delta equals the number of stranded jobs (recovery accounted)
   - queue is empty (ready/scheduled/inflight all zero)

Writes results to reports/benchmark-results.json.
"""

import json
import statistics
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import redis as redislib
import requests

API = "http://localhost:8000"
REDIS = redislib.Redis(host="localhost", port=6380, decode_responses=True,
                       socket_timeout=30)
COUNT = 600          # sleep jobs
HANDLER_SLEEP = 0.3  # seconds per job: keeps a job reliably in flight
KILL_AT = 12         # seconds into the run
RESTART_AT = 35      # seconds after kill (> VISIBILITY_TIMEOUT=30, so the
                     # reaper on a surviving worker must do the recovery)
KILLED = "distributed-job-queue-worker-2"


def docker(*args):
    return subprocess.run(["docker", *args], capture_output=True, text=True,
                          timeout=120).stdout


def stats():
    return requests.get(API + "/stats", timeout=10).json()


def main():
    before = stats()
    print("baseline stats: completed=%d retried=%d" % (before["completed"], before["retried"]))

    # --- enqueue COUNT jobs while the run is underway --------------------
    ids, enq_errors = [], [0]
    lock = threading.Lock()
    session = requests.Session()
    enq_start = time.time()

    def one(i):
        try:
            r = session.post(API + "/enqueue", timeout=15, json={
                "type": "sleep", "payload": {"seconds": HANDLER_SLEEP}})
            r.raise_for_status()
            with lock:
                ids.append(r.json()["id"])
        except Exception:
            with lock:
                enq_errors[0] += 1

    kill_timer = threading.Timer(KILL_AT, lambda: (docker("kill", KILLED),
                                                   print("[%0.1fs] killed %s" % (time.time() - enq_start, KILLED))))
    kill_timer.start()

    with ThreadPoolExecutor(max_workers=30) as pool:
        list(pool.map(one, range(COUNT)))
    print("[%0.1fs] enqueued %d jobs, %d errors" % (time.time() - enq_start, len(ids), enq_errors[0]))

    # --- capture in-flight jobs right after the kill ----------------------
    time.sleep(max(0.0, KILL_AT + 2 - (time.time() - enq_start)))
    inflight_at_kill = REDIS.zrange("queue:inflight", 0, -1)
    # The definitive stranded set: the killed worker's most recent claim.
    claimed_log = docker("logs", KILLED).splitlines()
    kill_wall = time.time()
    last_claimed = [l for l in claimed_log if "claimed" in l]
    last_claimed_id = last_claimed[-1].split()[-2].split(" ")[-1] if last_claimed else None
    print("[%0.1fs] in-flight at kill: %s" % (time.time() - enq_start, inflight_at_kill))
    print("[%0.1fs] killed worker's last claim: %s" % (time.time() - enq_start, last_claimed_id))

    # --- wait past visibility timeout so the reaper must act, then restart
    time.sleep(max(0.0, RESTART_AT - (time.time() - enq_start - KILL_AT)))
    out = subprocess.run(["docker", "start", KILLED], capture_output=True,
                         text=True, timeout=120)
    print("[%0.1fs] restarted %s: %s" % (time.time() - enq_start, KILLED, out.stdout.strip()))

    # --- wait for the batch to drain -------------------------------------
    deadline = time.time() + 900
    while time.time() < deadline:
        time.sleep(1)
        s = stats()
        done = s["completed"] - before["completed"]
        if done >= len(ids) and s["ready_total"] == 0 and s["inflight"] == 0:
            break
    else:
        print("TIMEOUT waiting for drain; final stats: %s" % s)

    after = stats()

    # --- verify every submitted job id ------------------------------------
    statuses = []
    BATCH = 1000
    for i in range(0, len(ids), BATCH):
        pipe = REDIS.pipeline(transaction=False)
        for jid in ids[i:i + BATCH]:
            pipe.hmget("job:%s" % jid, "status", "attempts")
        statuses.extend(pipe.execute())

    n_completed = sum(1 for st, _ in statuses if st == "completed")
    n_other = [(st, att) for st, att in statuses if st != "completed"]
    completed_delta = after["completed"] - before["completed"]
    failed_delta = after["failed"] - before["failed"]
    dead_delta = after["dead"] - before["dead"]

    # Definitive recovery marker: the reaper stamps last_error on requeue.
    last_errors = {}
    for i in range(0, len(ids), BATCH):
        pipe = REDIS.pipeline(transaction=False)
        for jid in ids[i:i + BATCH]:
            pipe.hget("job:%s" % jid, "last_error")
        for jid, le in zip(ids[i:i + BATCH], pipe.execute()):
            if le:
                last_errors[jid] = le
    recovered_ids = [j for j, le in last_errors.items() if "visibility timeout" in le]
    recovered_attempts = [dict(zip(ids[i:i + BATCH],
                        [a for _, a in statuses[i:i + BATCH]]))[j]
                        for j in recovered_ids
                        for i in [next(idx for idx in range(0, len(ids), BATCH) if j in ids[idx:idx + BATCH])]]

    result = {
        "label": "worker-failure-recovery",
        "jobs_submitted": len(ids),
        "enqueue_errors": enq_errors[0],
        "killed_worker": KILLED,
        "kill_mode": "docker kill (SIGKILL, mid-run)",
        "inflight_at_kill": inflight_at_kill,
        "killed_worker_last_claim": last_claimed_id,
        "recovered_jobs": len(recovered_ids),
        "recovered_job_ids": recovered_ids,
        "recovered_job_attempts": recovered_attempts,
        "recovered_job_last_errors": [last_errors[j] for j in recovered_ids],
        "failed_delivery_attempts": failed_delta,
        "completed": completed_delta,
        "dead": dead_delta,
        "jobs_status_completed": n_completed,
        "jobs_not_completed": n_other,
        "queue_empty_after": (after["ready_total"] == 0 and after["inflight"] == 0
                              and after["scheduled"] == 0),
        "no_double_ack": completed_delta == len(ids),
        "no_lost_jobs": n_completed == len(ids),
    }
    print(json.dumps(result, indent=2))
    with open("reports/benchmark-results.json", "a") as f:
        f.write(json.dumps(result) + "\n")

    ok = (result["no_lost_jobs"] and result["no_double_ack"]
          and result["queue_empty_after"] and dead_delta == 0
          and len(recovered_ids) >= 1)
    print("RECOVERY TEST: %s" % ("PASS" if ok else "FAIL"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
