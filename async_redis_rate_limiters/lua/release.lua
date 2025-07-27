local key = KEYS[1]
local channel = ARGV[1]
local client_id = ARGV[2]
local ttl = tonumber(ARGV[3])
local removed = redis.call('ZREM', key, client_id)
if removed == 1 then
    redis.call('PUBLISH', channel, 'release')
end
return removed
