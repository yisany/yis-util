package com.yis.util.redis.jedis;

import com.yis.util.redis.RedisPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

import java.util.Collections;

/**
 * March, or die.
 *
 * @Description:
 * @Created by yisany on 2020/03/26
 */
public class JedisUtil {

    private static final Logger log = LogManager.getLogger(JedisUtil.class);

    private static final String lockKey = "single_lock";
    // 锁过期时间
    private static final long internalLockeaseTime = 60 * 1000L;
    // 获取锁超时时间
    private static final long timeOut = 60 * 1000L;
    // 加锁条件
    private static final SetParams params = SetParams.setParams().nx().px(internalLockeaseTime);
    // 解锁Lua脚本, 保证原子性
    private static final String unlockScript =
            "if redis.call('get',KEYS[1]) == ARGV[1] then" +
                    "   return redis.call('del',KEYS[1]) " +
                    "else" +
                    "   return 0 " +
                    "end";

    /**
     * 设置值, 字符串类型
     * @param key
     * @param value
     * @return
     */
    public static String set(String key, String value) {
        Object o = execute(j -> j.set(key, value));
        return getString(o);
    }

    /**
     * 获取值, 字符串类型
     * @param key
     * @return
     */
    public static String get(String key) {
        Object o = execute((j -> j.get(key)));
        return getString(o);
    }

    /**
     * 设置锁
     *
     * @param key
     * @param value
     * @param exTime
     * @return
     */
    public static String setEx(String key, String value, int exTime) {
        Object o = execute(j -> j.setex(key, exTime, value));
        return getString(o);
    }

    /**
     * 设置过期时间
     *
     * @param key
     * @param exTime
     * @return
     */
    public static Long expire(String key, int exTime) {
        Object o = execute(j -> j.expire(key, exTime));
        return getLong(o);
    }

    /**
     * 删除键
     * @param key
     * @return
     */
    public static Long del(String key) {
        Object o = execute(j -> j.del(key));
        return getLong(o);
    }

    /**
     * 查询是否存在键
     * @param key
     * @return
     */
    public static Boolean exists(String key) {
        Object o = execute(j -> j.exists(key));
        return getBoolean(o);
    }

    /**
     * 分布式锁, 加锁
     * @param id
     * @return
     */
    public static boolean lock(String id) {
        Long start = System.currentTimeMillis();
        for (;;) {
            Object execute = execute(j -> j.set(lockKey, id, params));
            if ("OK".equals(getString(execute))) {
                // 加锁成功
                return true;
            }
            // 否则循环等待来获取锁, 如果在timeout内仍未获取到锁, 则失败
            long l = System.currentTimeMillis() - start;
            if (l > timeOut) {
                log.error("JedisUtil.lock, get lock error: timeOut, id={}", id);
                return false;
            }
            try {
                log.info("JedisUtil.lock, try to get lock, id={}", id);
                Thread.sleep(100);
            } catch (InterruptedException e) {
                log.error("JedisUtil.lock error, e={}", e);
            }
        }
    }

    /**
     * 分布式锁, 解锁
     * @param id
     * @return
     */
    public static boolean unlock(String id) {
        Object execute = JedisUtil.execute(j -> j.eval(unlockScript, Collections.singletonList(lockKey), Collections.singletonList(id)));
        if ("1".equals(getString(execute))) {
            return true;
        }
        return false;
    }

    /**
     * 通用执行方法
     * @param caller
     * @return
     */
    public static Object execute(Caller caller) {
        Jedis jedis = null;
        Object o = null;
        try {
            jedis = getJedis();
            o = caller.execute(jedis);
        } catch (RuntimeException e) {
            log.error("JedisUtil.execute error, e={}", e);
        } finally {
            close(jedis);
        }
        return o;
    }
    private static Boolean getBoolean(Object o) {
        return Boolean.parseBoolean(String.valueOf(o));
    }

    private static Integer getInt(Object o) {
        return Integer.parseInt(String.valueOf(o));
    }

    private static Long getLong(Object o) {
        return Long.parseLong(String.valueOf(o));
    }

    private static String getString(Object o) {
        return String.valueOf(o);
    }

    private static Jedis getJedis() {
        return RedisPool.getJedis();
    }

    private static void close(Jedis jedis) {
        RedisPool.returnResource(jedis);
    }

    interface Caller {
        Object execute(Jedis jedis);
    }


}
