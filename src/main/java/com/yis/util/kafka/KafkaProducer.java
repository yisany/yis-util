package com.yis.util.kafka;

import com.alibaba.fastjson.JSON;
import com.yis.util.date.DateUtil;
import com.yis.util.exception.BizException;
import com.yis.util.kafka.client.JKafkaProducer;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * @author milu
 * @Description kafka生产
 * @createTime 2019年12月12日 13:48:00
 */
public class KafkaProducer {

    private static final Logger logger = LogManager.getLogger(KafkaProducer.class);

    private static JKafkaProducer producer;
    private static volatile KafkaProducer handler;

    private Properties props;
    private String bootstrapServers;
    private Map<String, String> properties;
    private String topic;

    private KafkaProducer() {}

    public static boolean initInstance(String bootstrapServers, String topic, Map<String, String> properties) {
        if (handler == null) {
            synchronized (KafkaProducer.class) {
                if (handler == null) {
                    handler = new KafkaProducer();
                    handler.init(bootstrapServers, topic, properties);
                    return true;
                }
            }
        }
        return false;
    }

    public static KafkaProducer getInstance() {
        if (handler == null) {
            synchronized (KafkaProducer.class) {
                if (handler == null) {
                    logger.error("KafkaProducer is not inited, please init");
                    throw new BizException("init KafkaProducer first, use KafkaProducer.initInstance method");
                }
            }
        }
        return handler;
    }

    private void init(String bootstrapServers, String topic, Map<String, String> properties) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.properties = properties;

        prepare();
    }

    private void prepare() {
        try {

            if (props == null) {
                props = new Properties();
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                props.putAll(properties);
            }
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            producer = JKafkaProducer.init(props);
        } catch (Exception e) {
            logger.error("kafka producer init error", e);
            throw new RuntimeException("kafka producer init error");
        }
    }

    public void release() {
        if (producer != null) {
            producer.close();
            logger.warn("kafka producer release.");
        }
    }

    /**
     * kafka生产消息
     *
     * @param msg
     */
    public void pushToKafka(String msg) {
        producer.sendWithRetry(topic, UUID.randomUUID().toString(), msg);
    }

    public void pushToKafka(String msg, Caller caller) {
        Map<String, Object> event = caller.convert(msg);
        producer.sendWithRetry(topic, UUID.randomUUID().toString(), JSON.toJSONString(event));
    }

    /**
     * 添加字段
     * @return
     */
    private String convert(String message) {
        String timestamp = DateUtil.getUTC(System.currentTimeMillis());

        Map<String, Object> event = new HashMap<>();
        event.put("message", message);
        event.put("@timestamp", timestamp);
        event.put("timestamp", timestamp);
        event.put("appname", "bash_agent");
        event.put("keeptype", "7d");
        event.put("logtype", "no_log");
        event.put("tag", "shell");
        event.put("local_ip", "127.0.0.1");
        event.put("tenant_id", 1);
        event.put("hostname", "localhost");
        event.put("path", "/");
        event.put("agent_type", "bash_agent");

        return JSON.toJSONString(event);
    }

    /**
     * 数据转换
     */
    public interface Caller {
        Map<String, Object> convert(String message);

    }

}
