package com.yis.util.kafka;

import com.yis.util.exception.BizException;
import com.yis.util.kafka.client.JKafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * @author milu
 * @Description kafka消费
 * @createTime 2020年01月02日 11:07:00
 */
public class KafkaConsumer {

    private static final Logger logger = LogManager.getLogger(KafkaConsumer.class);

    private static long offsetCountInterval = Long.MAX_VALUE - 256;
    private static long offsetTimeIntervalMs = Long.MAX_VALUE - 256;
    private static int threadCount = 1;

    private static JKafkaConsumer consumer;
    private static volatile KafkaConsumer handler;

    private Properties props;
    private String bootstrapServers;
    private String topics;
    private String groupId;
    private Map<String, String> consumerSettings;

    private KafkaConsumer() {}

    public static boolean initInstance(String bootstrapServers, String topics, String groupId, Map<String, String> properties) {
        if (handler == null) {
            synchronized (KafkaConsumer.class) {
                if (handler == null) {
                    handler = new KafkaConsumer();
                    handler.init(bootstrapServers, topics, groupId, properties);
                    return true;
                }
            }
        }
        return false;
    }

    public static KafkaConsumer getInstance() {
        if (handler == null) {
            synchronized (KafkaConsumer.class) {
                if (handler == null) {
                    logger.error("KafkaConsumer is not inited, please init");
                    throw new BizException("init KafkaConsumer first, use KafkaConsumer.initInstance method");
                }
            }
        }
        return handler;
    }

    private void init(String bootstrapServers, String topics, String groupId, Map<String, String> properties) {
        this.bootstrapServers = bootstrapServers;
        this.topics = topics;
        this.groupId = groupId;
        this.consumerSettings = properties;

        prepare();
    }

    private void prepare() {
        props = generateConsumerProps();
        props.put("bootstrap.servers", bootstrapServers);
        consumer = JKafkaConsumer.init(props);
    }

    private Properties generateConsumerProps() {
        Properties props = new Properties();
        Iterator<Map.Entry<String, String>> setting = consumerSettings.entrySet().iterator();

        while (setting.hasNext()) {
            Map.Entry<String, String> entry = setting.next();
            props.put(entry.getKey(), entry.getValue());
        }

        return props;
    }

    public void release() {
        if (consumer != null) {
            consumer.close();
            logger.warn("kafka consumer release.");
        }
    }

    public void pullFromKafka(Caller caller) {
        try {
            consumer.add(topics, groupId, new JKafkaConsumer.Caller() {
                @Override
                public void processMessage(String message) {
                    caller.emit(message);
                }

                @Override
                public void catchException(String message, Throwable e) {
                    logger.warn("kakfa consumer fetch is error,message={}", message);
                    logger.error("kakfa consumer fetch is error", e);
                }
            }, Integer.MAX_VALUE, offsetCountInterval, offsetTimeIntervalMs, threadCount).execute();
        } catch (Exception e) {
            logger.error("kafka consumer pullFromKafka error, e={}", e);
        }
    }

    /**
     * 数据消费
     */
    public interface Caller {
        void emit(String message);

    }
}
