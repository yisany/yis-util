package com.yis;

import com.yis.util.kafka.KafkaConsumer;
import com.yis.util.kafka.KafkaProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class Main {

    private static Logger logger = LogManager.getLogger(Main.class);

    public static void main(String[] args) {
        String bootstrap = "172.16.8.151:9092";
        String topic = "milu_test";
        String groupId = "test";
        Map<String, String> props = new HashMap<>();

        KafkaProducer.initInstance(bootstrap, topic, props);
        KafkaProducer.getInstance().pushToKafka("xxx", message -> {
            Map<String, Object> event = new HashMap<>();
            event.put("message", message);
            return event;
        });

        KafkaConsumer.initInstance(bootstrap, topic, groupId, props);
        KafkaConsumer.getInstance().pullFromKafka(message -> {
            System.out.println(message);
        });
    }
}
