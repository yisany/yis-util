package com.yis.util.kafka.client;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class JKafkaConsumer {
    private static Logger logger = LoggerFactory.getLogger(JKafkaConsumer.class);
    private volatile Properties props;
    private static List<JKafkaConsumer> jconsumerList = new CopyOnWriteArrayList();
    private Map<String, JKafkaConsumer.Client> containers = new ConcurrentHashMap();
    private ExecutorService executor = Executors.newCachedThreadPool();
    private long offsetCountInterval = 9223372036854775551L;
    private long offsetTimeIntervalMs = 9223372036854775551L;

    public JKafkaConsumer(Properties props) {
        this.props = props;
    }

    public static JKafkaConsumer init(Properties p) throws IllegalStateException {
        Properties props = new Properties();
        props.put("max.poll.interval.ms", "86400000");
        props.put("connections.max.idle.ms", "30000");
        props.put("request.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        if (p != null) {
            props.putAll(p);
        }

        JKafkaConsumer jKafkaConsumer = new JKafkaConsumer(props);
        jconsumerList.add(jKafkaConsumer);
        return jKafkaConsumer;
    }

    public static JKafkaConsumer init(String bootstrapServer) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        return init(props);
    }

    public JKafkaConsumer add(String topics, String group, JKafkaConsumer.Caller caller, long timeout, long offsetCountInterval, long offsetTimeIntervalMs, int threadCount) {
        Properties clientProps = new Properties();
        clientProps.putAll(this.props);
        clientProps.put("group.id", group);
        if (threadCount < 1) {
            threadCount = 1;
        }

        for(int i = 0; i < threadCount; ++i) {
            this.containers.put(topics + "_" + i, new JKafkaConsumer.Client(clientProps, Arrays.asList(topics.split(",")), caller, timeout, offsetCountInterval, offsetTimeIntervalMs));
        }

        return this;
    }

    public JKafkaConsumer add(String topics, String group, JKafkaConsumer.Caller caller) {
        return this.add(topics, group, caller, 9223372036854775807L, this.offsetCountInterval, this.offsetTimeIntervalMs, 1);
    }

    public JKafkaConsumer add(String topics, String group, JKafkaConsumer.Caller caller, int threadCount) {
        return this.add(topics, group, caller, 9223372036854775807L, this.offsetCountInterval, this.offsetTimeIntervalMs, threadCount);
    }

    public void execute() {
        Iterator var1 = this.containers.entrySet().iterator();

        while(var1.hasNext()) {
            Entry<String, JKafkaConsumer.Client> c = (Entry)var1.next();
            this.executor.execute((Runnable)c.getValue());
        }

    }

    public void close() {
        Iterator var1 = this.containers.entrySet().iterator();

        while(var1.hasNext()) {
            Entry<String, JKafkaConsumer.Client> c = (Entry)var1.next();
            this.containers.remove(c.getKey());
            ((JKafkaConsumer.Client)c.getValue()).close();
        }

    }

    public static void closeAll() {
        Iterator var0 = jconsumerList.iterator();

        while(var0.hasNext()) {
            JKafkaConsumer c = (JKafkaConsumer)var0.next();
            c.close();
        }

    }

    public class Client implements Runnable {
        private long offsetCountInterval;
        private long offsetTimeIntervalMs;
        private long count = 0L;
        private long lasttime = 0L;
        private JKafkaConsumer.Caller caller;
        private volatile boolean running = true;
        private long pollTimeout;
        private KafkaConsumer<String, String> consumer;

        public Client(Properties clientProps, List<String> topics, JKafkaConsumer.Caller caller, long pollTimeout, long offsetCountInterval, long offsetTimeIntervalMs) {
            this.pollTimeout = pollTimeout;
            this.offsetCountInterval = offsetCountInterval;
            this.offsetTimeIntervalMs = offsetTimeIntervalMs;
            this.caller = caller;
            this.consumer = new KafkaConsumer(clientProps);
            this.consumer.subscribe(topics);
        }

        public void run() {
            label97:
            while(true) {
                try {
                    if (this.running) {
                        try {
                            ConsumerRecords<String, String> records = this.consumer.poll(9223372036854775807L);
                            Iterator var2 = records.iterator();

                            while(true) {
                                ConsumerRecord r;
                                do {
                                    do {
                                        if (!var2.hasNext()) {
                                            continue label97;
                                        }

                                        r = (ConsumerRecord)var2.next();
                                    } while(r.value() == null);
                                } while("".equals(r.value()));

                                try {
                                    this.caller.processMessage((String)r.value());
                                    long now = System.currentTimeMillis();
                                    if (++this.count > this.offsetCountInterval || now - this.lasttime > this.offsetTimeIntervalMs) {
                                        this.consumer.commitSync();
                                        this.count = 0L;
                                        this.lasttime = now;
                                    }
                                } catch (Throwable var10) {
                                    this.caller.catchException((String)r.value(), var10);
                                }
                            }
                        } catch (WakeupException var11) {
                            JKafkaConsumer.logger.warn("kafka consumer WakeupException", var11);
                            continue;
                        }
                    }
                } catch (Throwable var12) {
                    this.caller.catchException("", var12);
                } finally {
                    this.consumer.close();
                }

                return;
            }
        }

        public void close() {
            try {
                this.running = false;
                this.consumer.wakeup();
            } catch (Exception var2) {
                JKafkaConsumer.logger.error("close kafka consumer error", var2);
            }

        }
    }

    public interface Caller {
        void processMessage(String var1);

        void catchException(String var1, Throwable var2);
    }
}
