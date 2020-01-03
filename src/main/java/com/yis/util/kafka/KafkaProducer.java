package com.yis.util.kafka;

import com.alibaba.fastjson.JSON;
import com.yis.util.exception.BizException;
import com.yis.util.kafka.client.JKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * @author milu
 * @Description kafka生产
 * @createTime 2020年01月03日 11:04:00
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

    /**
     * 初始化生产者
     * @param bootstrapServers
     * @param topic
     * @param properties
     * @return
     */
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

    /**
     * 获取生产者实例
     * @return
     */
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

    /**
     * 关闭生产者
     */
    public void release() {
        producer.close();
        logger.info("kafka producer release.");
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

    /**
     * 数据发送
     * @param message
     */
    public void pushToKafka(String message) {
        producer.sendWithRetry(topic, UUID.randomUUID().toString(), message);
        producer.flush();
    }

    /**
     * 数据发送
     * @param message
     * @param caller
     */
    public void pushToKafka(String message, Caller caller) {
        Map<String, Object> event = caller.convert(message);
        producer.sendWithRetry(topic, UUID.randomUUID().toString(), JSON.toJSONString(event));
        producer.flush();
    }

    /**
     * 数据处理
     */
    public interface Caller {
        Map<String, Object> convert(String message);

    }

}
