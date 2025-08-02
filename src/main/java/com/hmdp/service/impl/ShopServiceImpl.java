package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.google.common.hash.BloomFilter;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.interceptor.BloomFilterInitializer;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;


    @Override
    public Result queryById(Long id) {
        // 缓存穿透解决方案
        // Shop shop = queryWithPassThrough(id);
        // 缓存击穿解决方案：互斥锁
        // Shop shop = queryWithMutex(id);
        // 缓存击穿解决方案：逻辑过期
        Shop shop = queryWithLogicalExpire(id);
        if (shop == null) {
            return Result.fail("店铺不存在！");
        }
        return Result.ok(shop);
    }
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    public Shop queryWithLogicalExpire(Long id) {
        String key = CACHE_SHOP_KEY + id;
        // 1.从redis查询商铺缓存
        String shopJSON = stringRedisTemplate.opsForValue().get(key);

        // 2.判断是否存在
        if (StrUtil.isBlank(shopJSON)) {
            // 3.不存在，直接返回
            return null;
        }

        // 4.命中，需要将json反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJSON, RedisData.class);;
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 5.判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 5.1.未过期，直接返回
            return shop;
        }
        // 5.2.过期，需要缓存重建
        // 6.缓存重建
        // 6.1.获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
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
                return JSONUtil.toBean((JSONObject) redisDataCheck.getData(), Shop.class);
            }
            // 6.4.开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    this.saveShop2Redis(id, 20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    // 释放锁
                    unLock(lockKey);
                }
            });

        }
        // 6.4.返回过期的缓存信息
        return shop;
    }

    public void saveShop2Redis(Long id, Long expireSeconds) throws InterruptedException {
        // 查询店铺数据
        Shop shop = this.getById(id);
        Thread.sleep(200);
        // 封装逻辑过期时间
        String key = CACHE_SHOP_KEY + id;
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        // 写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }
    public Shop queryWithMutex(Long id) {
        if (!BloomFilterInitializer.getBloomFilter().mightContain(id)) {
            log.debug("使用布隆过滤器查询到店铺不存在，id= {}", id);
            return null;
        }
        String key = CACHE_SHOP_KEY + id;
        // 1.从redis查询商铺缓存
        String shopJSON = stringRedisTemplate.opsForValue().get(key);

        // 2.判断是否存在
        if (StrUtil.isNotBlank(shopJSON)) {
            // 3.存在，直接返回
            Shop shop = JSONUtil.toBean(shopJSON, Shop.class);
            return shop;
        }
        // 判断命中的是否是空值
        if (shopJSON != null) {
            return null;
        }
        // 4.实现缓存重建
        Shop shop = null;
        // 4.1.获取互斥锁
        String lock = LOCK_SHOP_KEY + id;
        try {

            // 4.2.判断是否获取成功
            while (!tryLock(lock)) {
                // 4.3.失败则休眠并重试
                Thread.sleep(50);
            }
            // 4.4.成功，再次检查缓存是否存在
            shopJSON = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(shopJSON)) {
                // 4.5.存在，直接返回
                shop = JSONUtil.toBean(shopJSON, Shop.class);
                return shop;
            }
            // 判断命中的是否是空值
            if (shopJSON != null) {
                return null;
            }
            // 4.6.不存在，查询数据库
            shop = this.getById(id);
            // 模拟延时
            Thread.sleep(200);
            // 5.不存在，返回错误
            if (shop == null) {
                // 将空值写入Redis
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }

            // 6.存在，存入redis，增加过期时间
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 7.释放互斥锁
            unLock(lock);
        }
        // 8.返回
        return shop;
    }

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag); // 防止自动拆箱返回null
    }
    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }

    public Shop queryWithPassThrough(Long id) {
        if (!BloomFilterInitializer.getBloomFilter().mightContain(id)) {
            log.debug("使用布隆过滤器查询到店铺不存在，id= {}", id);
            return null;
        }
        String key = CACHE_SHOP_KEY + id;
        // 1.从redis查询商铺缓存
        String shopJSON = stringRedisTemplate.opsForValue().get(key);

        // 2.判断是否存在
        if (StrUtil.isNotBlank(shopJSON)) {
            // 3.存在，直接返回
            Shop shop = JSONUtil.toBean(shopJSON, Shop.class);
            return shop;
        }
        // 判断命中的是否是空值
        if (shopJSON != null) {
            return null;
        }

        // 4.不存在，根据id查数据库
        Shop shop = this.getById(id);
        // 5.不存在，返回错误
        if (shop == null) {
            // 将空值写入Redis
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }

        // 6.存在，存入redis，增加过期时间
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 7..返回
        return shop;
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        // 1.更新数据库
        updateById(shop);
        // 2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }
}
