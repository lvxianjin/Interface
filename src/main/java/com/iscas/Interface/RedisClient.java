package com.iscas.Interface;

import com.iscas.Configuration.Configuration;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @Author: lxj
 * @Date: 2019/3/18 9:36
 * @Version 1.0
 */
public class RedisClient {
    public static Jedis jedis = null;
    @Before
    public void init() {
        jedis = new Jedis(Configuration.RedisURL, Configuration.RedisPort);
    }
    @Test
    public String readRedis(String key) {
        String value = jedis.get(key);
        return value;
    }
    @Test
    public void writeRedis(Map<String, String> map) {
        Set<String> keys = map.keySet();// 得到全部的key
        Iterator<String> iter = keys.iterator();
        while (iter.hasNext()) {
            String key = iter.next();
            jedis.set(key, map.get(key));
        }
    }
}
