package com.yis;

import com.alibaba.fastjson.JSON;
import com.yis.util.cli.CliUtil;
import com.yis.util.jdbc.DbUtil;
import com.yis.util.kafka.KafkaConsumer;
import com.yis.util.kafka.KafkaProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
        KafkaProducer.getInstance().pushToKafka("erwerwer");

        KafkaConsumer.initInstance(bootstrap, topic, groupId, props);
        KafkaConsumer.getInstance().pullFromKafka(new KafkaConsumer.Caller() {
            @Override
            public void emit(String message) {
                System.out.println(message);
            }
        });
    }
}
