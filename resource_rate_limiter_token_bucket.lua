-- Single threaded Token Bucket implementation (without blocking)
-- Reference: https://en.wikipedia.org/wiki/Token_bucket
-- Keys: tokens_key, timestamp_key
-- Args: rate, capacity, bucket_init_num
-- Returns: allowed
-- Redis CLI (with debug):  redis-cli --ldb --eval request_rate_limiter.lua ff ff-ts , 0.5 5 1

-- This is required to be able to use TIME and writes; basically it lifts the script into IO
redis.replicate_commands()

local tokens_key = KEYS[1]
local timestamp_key = KEYS[2]
--redis.log(redis.LOG_WARNING, "tokens_key " .. tokens_key)

-- Example:  100 r/s , 0.1245 r/s
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local bucket_init_num = tonumber(ARGV[3])
local requested = 1 --api requested limited (default = 1)

-- redis returns time as an array containing two integers:
-- 1. seconds of the epoch time (10 digits)
-- 2. microseconds (6 digits)
-- for convenience we need to convert them to a floating point number.
-- the resulting number is 16 digits, bordering on the limits of a 64-bit double-precision floating point number.
-- adjust the epoch to be relative to 2022-10-01 00:00:00 GMT to avoid floating point problems
local start_time = 1664582400
-- Take a timestamp
local redis_time = redis.call("TIME") -- Array of [seconds, microseconds]
-- Lua script minimum unit to 100 microseconds(μs)
local now = tonumber(redis_time[1] - start_time) + (tonumber(redis_time[2]) / 1000000)

local ttl = math.ceil((capacity / rate) * 2)

--redis.log(redis.LOG_WARNING, "now " .. now)
--redis.log(redis.LOG_WARNING, "rate " .. rate)
--redis.log(redis.LOG_WARNING, "capacity " .. capacity)
--redis.log(redis.LOG_WARNING, "bucket_init_num " .. bucket_init_num)
--redis.log(redis.LOG_WARNING, "requested " .. requested)
--redis.log(redis.LOG_WARNING, "ttl " .. ttl)

local last_tokens = tonumber(redis.call("get", tokens_key))  or bucket_init_num -- use sensible default of 'bucket_init_num' if the key does not exist
--redis.log(redis.LOG_WARNING, "last_tokens " .. last_tokens)

local last_refreshed = tonumber(redis.call("get", timestamp_key)) or now -- use sensible default of 'now' if the key does not exist
--redis.log(redis.LOG_WARNING, "last_refreshed " .. last_refreshed)

if now < last_refreshed then
  redis.log(redis.LOG_WARNING, "redis server clock back, last_refreshed " .. last_refreshed)
end

local delta = math.max(0, now-last_refreshed)
local filled_tokens = math.min(capacity, last_tokens + (delta * rate))
local allowed = filled_tokens >= requested
local new_tokens = filled_tokens
local allowed_num = 0
if allowed then
  new_tokens = filled_tokens - requested
  allowed_num = 1
end

--redis.log(redis.LOG_WARNING, "delta " .. delta)
--redis.log(redis.LOG_WARNING, "filled_tokens " .. filled_tokens)
--redis.log(redis.LOG_WARNING, "allowed_num " .. allowed_num)
--redis.log(redis.LOG_WARNING, "new_tokens " .. new_tokens)

if ttl > 0 then
  redis.call("setex", tokens_key, ttl, new_tokens)
  redis.call("setex", timestamp_key, ttl, now)
end
--redis.log(redis.LOG_WARNING, "=========================")
-- return { allowed_num, new_tokens, capacity, filled_tokens, requested, new_tokens }
return allowed_num