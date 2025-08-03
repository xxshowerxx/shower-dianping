package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import com.hmdp.interceptor.BloomFilterInitializer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Component
@Slf4j
public class CacheClient {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    private final static Long NULL_TTL = 2L;
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10L, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag); // 防止自动拆箱返回null
    }
    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }
    public <R, ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback,
                                       Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1.从redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);

        // 2.判断是否存在
        if (StrUtil.isBlank(json)) {
            // 3.不存在，直接返回
            return null;
        }

        // 4.命中，需要将json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);;
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 5.判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 5.1.未过期，直接返回
            return r;
        }
        // 5.2.过期，需要缓存重建
        // 6.缓存重建
        // 6.1.获取互斥锁
        String lockKey = "LOCK_" + keyPrefix + id;
        boolean isLock = tryLock(lockKey);
        // 6.2.判断是否获取锁成功
        if (isLock) {
            // 6.3.成功，再次检测缓存是否过期，DoubleCheck
            String cache = stringRedisTemplate.opsForValue().get(key);
            RedisData redisDataCheck = JSONUtil.toBean(cache, RedisData.class);
            LocalDateTime expireTimeCheck = redisDataCheck.getExpireTime();
            if (expireTimeCheck.isAfter(LocalDateTime.now())) {
                // 如果发现已经被其他线程重建了缓存，则直接释放锁并返回
                unLock(lockKey);
                return JSONUtil.toBean((JSONObject) redisDataCheck.getData(), type);
            }
            // 6.4.开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    R r1 = dbFallback.apply(id);
                    setWithLogicalExpire(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    // 释放锁
                    unLock(lockKey);
                }
            });

        }
        // 6.4.返回过期的缓存信息
        return r;
    }


    public <R, ID> R queryWithMutex(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback,
                               Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1.从redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);

        // 2.判断是否存在
        if (StrUtil.isNotBlank(json)) {
            // 3.存在，直接返回
            R r = JSONUtil.toBean(json, type);
            return r;
        }
        // 判断命中的是否是空值
        if (json != null) {
            return null;
        }
        // 4.实现缓存重建
        R r = null;
        // 4.1.获取互斥锁
        String lock = "LOCK_" + keyPrefix + id;
        try {

            // 4.2.判断是否获取成功
            while (!tryLock(lock)) {
                // 4.3.失败则休眠并重试
                Thread.sleep(50);
            }
            // 4.4.成功，再次检查缓存是否存在
            json = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(json)) {
                // 4.5.存在，直接返回
                r = JSONUtil.toBean(json, type);
                return r;
            }
            // 判断命中的是否是空值
            if (json != null) {
                return null;
            }
            // 4.6.不存在，查询数据库
            r = dbFallback.apply(id);
            // 模拟延时
            Thread.sleep(200);
            // 5.不存在，返回错误
            if (r == null) {
                // 将空值写入Redis
                stringRedisTemplate.opsForValue().set(key, "", NULL_TTL, TimeUnit.MINUTES);
                return null;
            }

            // 6.存在，存入redis，增加过期时间
            this.set(key, r, time, unit);

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 7.释放互斥锁
            unLock(lock);
        }
        // 8.返回
        return r;
    }



    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback,
                                          Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1.从redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);

        // 2.判断是否存在
        if (StrUtil.isNotBlank(json)) {
            // 3.存在，直接返回
            return JSONUtil.toBean(json, type);
        }
        // 判断命中的是否是空值
        if (json != null) {
            return null;
        }

        // 4.不存在，根据id查数据库
        R r = dbFallback.apply(id);
        // 5.不存在，返回错误
        if (r == null) {
            // 将空值写入Redis
            stringRedisTemplate.opsForValue().set(key, "", NULL_TTL, TimeUnit.MINUTES);
            return null;
        }

        // 6.存在，存入redis，增加过期时间
        this.set(key, r, time, unit);
        // 7..返回
        return r;
    }

}
