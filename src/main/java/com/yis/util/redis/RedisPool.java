package com.yis.util.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * March, or die.
 *
 * @Description:
 * @Created by yisany on 2020/03/26
 */
public class RedisPool {
    private static JedisPool pool;//jedis连接池
    private static Integer maxTotal = 20; //最大连接数
    private static Integer maxIdle = 10;//在jedispool中最大的idle状态(空闲的)的jedis实例的个数
    private static Integer minIdle = 2;//在jedispool中最小的idle状态(空闲的)的jedis实例的个数

    private static Boolean testOnBorrow = true;
    private static Boolean testOnReturn = true;

    private static String redisIp = "127.0.0.1";
    private static Integer redisPort = 6379;
    private static String password = "123456";
    private static Integer database = 1;


    private static void initPool(){
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(maxTotal);
        config.setMaxIdle(maxIdle);
        config.setMinIdle(minIdle);

        config.setTestOnBorrow(testOnBorrow);
        config.setTestOnReturn(testOnReturn);

        config.setBlockWhenExhausted(true);//连接耗尽的时候，是否阻塞，false会抛出异常，true阻塞直到超时。默认为true。

        pool = new JedisPool(config,redisIp,redisPort,1000*2, password, database);
    }

    //静态代码块，初始化Redis池
    static{
        initPool();
    }

    public static Jedis getJedis(){
        return pool.getResource();
    }

    public static void returnResource(Jedis jedis) {
        jedis.close();
    }

}
