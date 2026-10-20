-- fail.lua: atomically handle a failed job, either scheduling a backoff retry
-- or moving it to the dead-letter list.
--
-- Like ack, this must be atomic against the reaper. We only act if the job is
-- still inflight (this worker still owns the lease). If the lease already
-- expired we do nothing: the reaper has, or will, requeue it, and acting here
-- would retry or dead-letter a job some other worker may now own.
--
-- The attempt counter is incremented here, inside the script, so the
-- read-increment-decide sequence is one indivisible step. "attempts" counts
-- concluded failed deliveries (handler failures plus visibility timeouts), and
-- reap.lua uses the very same counter and limit, so a job dies after
-- MAX_ATTEMPTS failures no matter how those failures happened.
--
-- KEYS[1]: inflight key.
-- KEYS[2]: job-hash key, e.g. "job:<id>".
-- KEYS[3]: scheduled key (ZSET, score = run-at epoch) for the retry path.
-- KEYS[4]: dead-letter key (LIST) for the give-up path.
-- KEYS[5]: failed counter key.
-- ARGV[1]: job id.
-- ARGV[2]: max attempts.
-- ARGV[3]: backoff base seconds.
-- ARGV[4]: backoff max seconds (cap).
-- ARGV[5]: now (unix epoch seconds).
-- ARGV[6]: error string from the handler.
-- ARGV[7]: result ttl in seconds (applied only on the dead-letter path).
--
-- Returns "retry", "dead", or "lost" (lost == lease already gone, did nothing).

local inflight_key = KEYS[1]
local job_key = KEYS[2]
local scheduled_key = KEYS[3]
local dead_key = KEYS[4]
local failed_key = KEYS[5]

local job_id = ARGV[1]
local max_attempts = tonumber(ARGV[2])
local backoff_base = tonumber(ARGV[3])
local backoff_max = tonumber(ARGV[4])
local now = tonumber(ARGV[5])
local err = ARGV[6]
local ttl = ARGV[7]

-- Drop the lease. If it was not ours, bail out without side effects.
local removed = redis.call("ZREM", inflight_key, job_id)
if removed == 0 then
  return "lost"
end

-- Count this failed delivery.
local attempts = redis.call("HINCRBY", job_key, "attempts", 1)
redis.call("INCR", failed_key)
redis.call("HSET", job_key, "last_error", err, "failed_at", now)

if attempts >= max_attempts then
  -- Out of retries. Park it in the dead-letter list for inspection.
  redis.call("HSET", job_key, "status", "dead")
  redis.call("LPUSH", dead_key, job_id)
  redis.call("EXPIRE", job_key, ttl)
  return "dead"
end

-- Still have retries left. Exponential backoff: base * 2^(attempts-1), capped.
-- First retry waits base, the next 2*base, then 4*base, and so on.
local delay = backoff_base * (2 ^ (attempts - 1))
if delay > backoff_max then
  delay = backoff_max
end
local retry_at = now + delay

-- Push into the scheduled set; the promoter moves it back to a ready queue
-- once retry_at arrives.
redis.call("HSET", job_key, "status", "scheduled")
redis.call("ZADD", scheduled_key, retry_at, job_id)
return "retry"
