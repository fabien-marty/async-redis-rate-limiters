local key = KEYS[1]
local client_id = ARGV[1]
local limit = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
local expires_at = now + ttl
redis.call('ZREMRANGEBYSCORE', key, '-inf', now)
redis.call('ZADD', key, expires_at, client_id)
redis.call('EXPIRE', key, ttl + 10)
local card = redis.call('ZCARD', key)
if card <= limit then
    return 1
else
    redis.call('ZREM', key, client_id)
    return 0
end
