-- promote.lua: move scheduled jobs whose run-at time has arrived into the
-- correct ready queue. Drives both delayed/scheduled enqueues and backoff
-- retries, since both park jobs in the scheduled ZSET keyed by run-at.
--
-- Done in one script so a job cannot be observed in both the scheduled set and
-- a ready queue at the same time, and cannot be promoted twice by two promoter
-- loops racing.
--
-- KEYS[1]: scheduled key (ZSET, score = run-at epoch).
-- KEYS[2]: sequence key (counter that scores ready-queue order, FIFO).
-- ARGV[1]: now (unix epoch seconds).
-- ARGV[2]: job-hash key prefix, e.g. "job:".
-- ARGV[3]: ready-queue key prefix, e.g. "queue:ready:".
-- ARGV[4]: default priority, used if a job somehow lost its priority field.
-- ARGV[5]: max jobs to promote in one call, bounds how long the script runs.
--
-- Returns the number of jobs promoted.

local scheduled_key = KEYS[1]
local seq_key = KEYS[2]
local now = ARGV[1]
local job_prefix = ARGV[2]
local ready_prefix = ARGV[3]
local default_priority = ARGV[4]
local limit = tonumber(ARGV[5])

-- All scheduled jobs that are due now or earlier, oldest first, capped.
local due = redis.call("ZRANGEBYSCORE", scheduled_key, "-inf", now, "LIMIT", 0, limit)
local promoted = 0

for _, job_id in ipairs(due) do
  -- Remove from scheduled first. ZREM returning 0 means another promoter just
  -- grabbed this id, so we skip it.
  if redis.call("ZREM", scheduled_key, job_id) == 1 then
    local job_key = job_prefix .. job_id
    local priority = redis.call("HGET", job_key, "priority")
    if not priority then
      priority = default_priority
    end
    -- A monotonically increasing score gives FIFO ordering inside the level.
    local seq = redis.call("INCR", seq_key)
    redis.call("ZADD", ready_prefix .. priority, seq, job_id)
    redis.call("HSET", job_key, "status", "ready")
    promoted = promoted + 1
  end
end

return promoted
