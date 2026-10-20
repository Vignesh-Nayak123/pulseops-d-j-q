-- ack.lua: atomically acknowledge a job as completed.
--
-- The worker calls this only after the handler returned successfully. Removing
-- the id from inflight and marking the job done must happen together: if we
-- removed it from inflight but crashed before marking done, the job would look
-- lost; if we marked done but left it in inflight, the reaper would requeue an
-- already-finished job. One script, so both happen or neither does.
--
-- KEYS[1]: inflight key.
-- KEYS[2]: job-hash key, e.g. "job:<id>".
-- KEYS[3]: completed counter key.
-- ARGV[1]: now (unix epoch seconds).
-- ARGV[2]: result ttl in seconds.
-- ARGV[3]: job id (the member to remove from inflight).
--
-- Returns 1 if the job was still inflight and got acked, 0 if it was not
-- inflight anymore (for example the reaper already requeued it, in which case
-- another worker owns it now and we must not double-count it as completed).

local inflight_key = KEYS[1]
local job_key = KEYS[2]
local completed_key = KEYS[3]
local now = ARGV[1]
local ttl = ARGV[2]

-- ZREM returns the number of members actually removed. If it is 0 this worker
-- no longer owns the job (its lease expired and the reaper took it back), so we
-- silently drop the ack rather than corrupt the counters.
local removed = redis.call("ZREM", inflight_key, ARGV[3])
if removed == 0 then
  return 0
end

redis.call("HSET", job_key, "status", "completed", "completed_at", now)
redis.call("INCR", completed_key)
-- Let the finished record expire so Redis does not grow without bound, while
-- still leaving the status queryable for a while after completion.
redis.call("EXPIRE", job_key, ttl)
return 1
