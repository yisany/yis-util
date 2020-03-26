package com.yis;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yis.util.cli.CliUtil;
import com.yis.util.encode.CodecUtil;
import com.yis.util.encode.SnowFlake;
import com.yis.util.jdbc.DbUtil;
import com.yis.util.kafka.KafkaConsumer;
import com.yis.util.kafka.KafkaProducer;
import com.yis.util.redis.RedisUtil;
import com.yis.util.shell.ShellUtil;
import com.yis.util.yaml.YamlUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

public class Main {

    private static Logger logger = LogManager.getLogger(Main.class);

    public static void main(String[] args) {
        Main main = new Main();

        // kafka测试
//        main.testKafka();
        // cli测试
//        main.testCli(args);
        // db测试
//        main.testJDBC();
        // yaml测试
//        main.testYaml();
        // redis测试
//        main.testRedis();
        // shell测试
//        main.testShell();
        // 雪花加密测试
//        main.testSnow();

        main.testCodec();

    }

    private void testCodec() {
        String charset = "UTF-8";
        JSONObject obj = new JSONObject();
        obj.put("ids", "43,56");
        obj.put("os", "linux");
        obj.put("token", "kRhisbdoTQMCZU5KqQqGkQ7sDA7BM9kpldnQ5Nf2al8ER9yp");
        obj.put("dir", "");
        JSONObject obj2 = new JSONObject();
        obj2.put("ids", "43,56");
//        obj2.put("os", "linux");
        obj2.put("token", "kRhisbdoTQMCZU5KqQqGk");
//        obj2.put("dir", "");
        try {
            String encodeParamStr = CodecUtil.urlEncode(CodecUtil.base64Encode(obj.toString(), charset), charset);
            String encodeParamStr2 = CodecUtil.urlEncode(CodecUtil.base64Encode(obj2.toString(), charset), charset);
            System.out.println(encodeParamStr);
            System.out.println(encodeParamStr2);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    private void testSnow() {
        SnowFlake snowFlake = new SnowFlake(2, 3);

        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            System.out.println(snowFlake.nextId());
        }

        System.out.println(System.currentTimeMillis() - start);


    }

    private void testShell() {
        ShellUtil.script("chmod", "755", "/Users/milu/data/tmp/shell/sh1.sh");
    }

    private void testRedis() {
        String host = "172.16.8.132";
        int port = 6379;
        String password = "abc123";
        RedisUtil.initInstance(host, port, password);
        String set = RedisUtil.getRedisUtil().set("test", "lallala");
        System.out.println(set);
    }

    private void testYaml() {
        Map<String, Object> yaml = YamlUtil.parseYaml("application.yaml");
        System.out.println(JSON.toJSONString(yaml));
    }

    private void testJDBC() {
        String ip = "localhost";
        int port = 3306;
        String dbName = "mysql";
        String user = "root";
        String password = "root";
        DbUtil.initInstance(ip, String.valueOf(port), dbName, user, password);
    }

    private void testCli(String[] args) {
        Map<String, String> clis = CliUtil.parseCli(args, "c", "w");
        System.out.println(JSON.toJSONString(clis));
    }

    private void testKafka() {
        String bootstrap = "172.16.8.151:9092";
        String topic = "milu_test";
        String groupId = "test_10006";
        Map<String, String> props = new HashMap(){{
            put("max.request.size", "20971520");
            put("compression.type", "lz4");
            put("request.timeout.ms", "86400000");
            put("retries", "1000000");
            put("max.in.flight.requests.per.connection", "1");
//            put("auto.offset.reset", "earliest"); // 从头消费
        }};

        KafkaProducer.initInstance(bootstrap, topic, props);
        KafkaProducer.getInstance().pushToKafka("lalal", (message, event) -> event.put("message", message));

        KafkaConsumer.initInstance(bootstrap, topic, groupId, props);
        KafkaConsumer.getInstance().pullFromKafka(message -> System.out.println(message));
    }
}
