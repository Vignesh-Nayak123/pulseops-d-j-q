-- claim.lua: atomically claim the next ready job, highest priority first.
--
-- Why this is race free:
-- Redis executes a Lua script as a single, indivisible unit. The server is
-- single threaded for command execution, and while EVAL runs no other client's
-- commands can interleave. So the whole sequence below (look at the ready
-- queues, pick a job, remove it, record it as inflight, flip its status) is one
-- atomic step. There is no window between "I saw this job id" and "I took this
-- job id" for a second worker to observe the same id. Contrast a naive claim in
-- application code: ZRANGE to peek, then ZREM to take. Two workers could both
-- ZRANGE the same id before either ZREMs it, and both would run it. Folding the
-- peek and the take into one script closes that window completely.
--
-- KEYS: one ready-queue key per priority, in priority order (highest first),
--       followed by the inflight key as the LAST key.
-- ARGV[1]: visibility deadline (unix epoch seconds) = now + visibility_timeout.
-- ARGV[2]: now (unix epoch seconds), stored on the job for observability.
-- ARGV[3]: job-hash key prefix, e.g. "job:".
--
-- Returns the claimed job id, or false if every ready queue is empty.

local inflight_key = KEYS[#KEYS]
local deadline = ARGV[1]
local now = ARGV[2]
local job_prefix = ARGV[3]

-- Walk the ready queues from highest priority to lowest. The first non-empty
-- one wins, so a single high job is always taken before any default or low job.
for i = 1, #KEYS - 1 do
  local ready_key = KEYS[i]
  -- ZPOPMIN removes and returns the member with the smallest score. We score
  -- ready jobs by an increasing sequence number, so smallest score == oldest,
  -- which gives FIFO order within a priority level.
  local popped = redis.call("ZPOPMIN", ready_key)
  if popped[1] then
    local job_id = popped[1]
    -- Record the job as inflight, scored by its visibility deadline. The reaper
    -- later scans this set for entries whose deadline has passed.
    redis.call("ZADD", inflight_key, deadline, job_id)
    -- Flip job state so /jobs/<id> reflects reality. We deliberately do NOT
    -- bump the attempt counter here. "attempts" counts concluded failed
    -- deliveries, and it is incremented in exactly one place per outcome:
    -- fail.lua on a handler failure, reap.lua on a visibility timeout. Bumping
    -- it here too would double-count a crashed job (once by the reaper, once
    -- when it is re-claimed).
    local job_key = job_prefix .. job_id
    redis.call("HSET", job_key, "status", "running", "claimed_at", now)
    return job_id
  end
end

return false
