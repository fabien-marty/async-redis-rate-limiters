local key = KEYS[1]
local list = KEYS[2]
local client_id = ARGV[1]
local limit = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
local expires_at = now + ttl
local removed = redis.call('ZREM', key, client_id)
if removed == 1 then
    redis.call('RPUSH', list, expires_at)
    redis.call('LTRIM', list, 0, limit - 1)
    redis.call('EXPIRE', list, ttl)
end

return removed
