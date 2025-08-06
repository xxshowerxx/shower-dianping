---@diagnostic disable: undefined-global
---
--- Created by showe.
--- DateTime: 2025/8/6 12:12
---

--获取锁种的线程标示 get key
local id = redis.call('get', KEYS[1])
-- 比较线程标示与锁中的标示是否一致
if (id == ARGV[1]) then
    -- 释放锁 del key
    return redis.call('del', KEYS[1])
end
return 0