package com.yis.util.redis.jedis;

import com.yis.util.redis.RedisPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Jedis;

/**
 * March, or die.
 *
 * @Description:
 * @Created by yisany on 2020/03/26
 */
public class JedisUtil {

    private static final Logger log = LogManager.getLogger(JedisUtil.class);

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
