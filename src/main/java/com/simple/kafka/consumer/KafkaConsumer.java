package com.simple.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by zg on 2016/11/3.
 *
 * @apiNote kafka-client-0.10.0.1
 */
public class KafkaConsumer<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    private org.apache.kafka.clients.consumer.KafkaConsumer<K, V> consumer;
    private Properties properties = new Properties();


    /**
     * 初始化默认配置
     */
    private void initProperties() {
        //props.put("auto.offset.reset", "earliest");
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        //props.put("group.id", groupId);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }


    /**
     * 初始化KafkaConsumer
     *
     * @param props default config:
     *              <p>
     *              "auto.offset.reset", "earliest"     restOffset,default config is ignore</br>
     *              "bootstrap.servers", "localhost:9092"</br>
     *              "group.id", "groupId"               default groupId is random</br>
     *              "key.serializer", "org.apache.kafka.common.serialization.StringSerializer"</br>
     *              "value.serializer", "org.apache.kafka.common.serialization.StringSerializer"</br>
     */
    public KafkaConsumer(Properties props) {
        initProperties();
        if (props != null) {
            this.properties = props;
        }

    }

    /**
     * init kafkaconsumer
     */
    public KafkaConsumer() {
        initProperties();
    }


    /**
     * @param servers eg: host:port,host:port
     * @return
     */
    public KafkaConsumer setServers(String servers) {
        this.properties.put("bootstrap.servers", servers);
        return this;
    }


    public KafkaConsumer setGroupId(String groupId) {
        this.properties.put("group.id", groupId);
        return this;
    }

    public KafkaConsumer setStringSeries() {
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return this;
    }

    public KafkaConsumer setOffsetReset(OffsetResetStrategy strategy) {
        properties.put("auto.offset.reset", strategy.toString().toLowerCase());
        return this;
    }

    /**
     * @return build kafkaConsumer
     */
    public KafkaConsumer build() {
        try {
            this.consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(this.properties);
        } catch (Exception e) {
            logger.error("KafkaConsumer init error:{}", e);
        }
        return this;
    }


    /**
     * 消费Kafka数据
     *
     * @param topics
     * @param msgPool     接收函数
     * @param pollTimeout The time, in milliseconds, spent waiting in poll if data is not available in the buffer.
     *                    If 0, returns immediately with any records that are available currently in the buffer, else returns empty.
     *                    Must not be negative.
     */
    public void consumer(List<String> topics, long pollTimeout, ConsumerMsgInterface msgPool) {
        try {
            this.consumer.subscribe(topics);
            while (true) {
                ConsumerRecords<K, V> records = this.consumer.poll(pollTimeout);
                for (ConsumerRecord<K, V> record : records) {
                    msgPool.receiveMsg(record.value().toString());
                }
            }
        } catch (Exception e) {
            logger.error("KafkaConsumer consumering error:{}", e);
        } finally {
            this.consumer.close();
        }
    }


    /**
     * 消费Kafka数据
     *
     * @param topic
     * @param msgPool     接收函数
     * @param pollTimeout The time, in milliseconds, spent waiting in poll if data is not available in the buffer.
     *                    If 0, returns immediately with any records that are available currently in the buffer, else returns empty.
     *                    Must not be negative.
     */
    public void consumer(String topic, long pollTimeout, ConsumerMsgInterface msgPool) {
        List<String> topics = new ArrayList<>();
        topics.add(topic);
        consumer(topics, pollTimeout, msgPool);
    }


    public void consumer(int threadCount, String topic, long pollTimeout, ConsumerMsgInterface msgPool) {
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        final List<MultiConsumer> consumers = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            MultiConsumer consumer = new MultiConsumer(topic, pollTimeout, msgPool);
            consumers.add(consumer);
            executor.submit(consumer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                consumers.forEach(MultiConsumer::shutdown);
                executor.shutdown();
                try {
                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    logger.error("consumer close error:{}", e);
                }
            }
        });
    }

    public void close() {
        consumer.close();
    }


    public class MultiConsumer implements Runnable {

        private String topic;
        private ConsumerMsgInterface msgPool;
        private long pollTimeout;

        public MultiConsumer(String topic, long pollTimeout, ConsumerMsgInterface msgPool) {
            this.topic = topic;
            this.msgPool = msgPool;
            this.pollTimeout = pollTimeout;
        }

        @Override
        public void run() {
            try {
                consumer(topic, pollTimeout, msgPool);
            } catch (Exception e) {
                logger.error("consumer error:{}", e);
            } finally {
                consumer.close();
            }
        }

        public void shutdown() {
            consumer.wakeup();
        }
    }

}
