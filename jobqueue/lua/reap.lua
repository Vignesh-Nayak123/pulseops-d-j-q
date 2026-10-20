-- reap.lua: requeue jobs whose visibility lease has expired.
--
-- This is the at-least-once safety net. When a worker claims a job we put its
-- id in the inflight ZSET scored by a deadline (now + visibility timeout). A
-- healthy worker removes the id when it acks or fails. If the worker crashes or
-- hangs, nobody removes it, the deadline passes, and this script finds it and
-- makes it visible again so another worker can run it. That is exactly why
-- delivery is at-least-once and not at-most-once: we would rather run a job
-- twice than lose it.
--
-- Done atomically so a job cannot be simultaneously left inflight and also
-- pushed back to ready, and so two reaper loops cannot both requeue the same id.
--
-- Poison-job guard: every timed-out delivery increments the SAME attempt
-- counter that handler failures use, and is checked against the SAME
-- MAX_ATTEMPTS limit. So a job that keeps crashing its worker (and therefore
-- keeps timing out) is requeued only until it has burned MAX_ATTEMPTS
-- deliveries, then it goes to the dead-letter list instead of looping forever.
--
-- KEYS[1]: inflight key (ZSET, score = visibility deadline).
-- KEYS[2]: sequence key (scores ready-queue FIFO order).
-- KEYS[3]: dead-letter key (LIST).
-- KEYS[4]: failed counter key.
-- ARGV[1]: now (unix epoch seconds).
-- ARGV[2]: job-hash key prefix, e.g. "job:".
-- ARGV[3]: ready-queue key prefix, e.g. "queue:ready:".
-- ARGV[4]: default priority fallback.
-- ARGV[5]: max attempts.
-- ARGV[6]: result ttl seconds (applied on the dead-letter path).
-- ARGV[7]: max jobs to reap in one call.
--
-- Returns a two element array {requeued_count, dead_count}.

local inflight_key = KEYS[1]
local seq_key = KEYS[2]
local dead_key = KEYS[3]
local failed_key = KEYS[4]

local now = ARGV[1]
local job_prefix = ARGV[2]
local ready_prefix = ARGV[3]
local default_priority = ARGV[4]
local max_attempts = tonumber(ARGV[5])
local ttl = ARGV[6]
local limit = tonumber(ARGV[7])

local expired = redis.call("ZRANGEBYSCORE", inflight_key, "-inf", now, "LIMIT", 0, limit)
local requeued = 0
local dead = 0

for _, job_id in ipairs(expired) do
  -- Claim the right to handle this expiry. If ZREM returns 0 another reaper, or
  -- a late ack from the original worker, already took it, so skip.
  if redis.call("ZREM", inflight_key, job_id) == 1 then
    local job_key = job_prefix .. job_id
    -- A timeout is a failed delivery, so count it like any other failure.
    local attempts = redis.call("HINCRBY", job_key, "attempts", 1)
    if attempts >= max_attempts then
      -- Burned all its attempts via timeouts. Dead-letter it so a job that
      -- repeatedly kills its worker cannot loop forever.
      redis.call("HSET", job_key, "status", "dead", "last_error", "visibility timeout, max attempts reached")
      redis.call("LPUSH", dead_key, job_id)
      redis.call("INCR", failed_key)
      redis.call("EXPIRE", job_key, ttl)
      dead = dead + 1
    else
      -- Still has attempts left. Make it visible again right away.
      local priority = redis.call("HGET", job_key, "priority")
      if not priority then
        priority = default_priority
      end
      local seq = redis.call("INCR", seq_key)
      redis.call("ZADD", ready_prefix .. priority, seq, job_id)
      redis.call("HSET", job_key, "status", "ready", "last_error", "visibility timeout, requeued")
      requeued = requeued + 1
    end
  end
end

return {requeued, dead}
