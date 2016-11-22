package com.simple.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by frank on 2016/11/3.
 *
 * @apiNote Simple KafkaConsumer
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
        properties.put("bootstrap.servers", "localhost:9092");
        //props.put("group.id", groupId);
        properties.put("key.serializer", StringDeserializer.class.toString());
        properties.put("value.serializer", StringDeserializer.class.toString());
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
        if (props == null) {
            props = this.properties;
        }
        try {
            this.consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
        } catch (Exception e) {
            logger.error("KafkaConsumer init error:{}", e);
        }
    }


    /**
     * 消费Kafka数据
     *
     * @param topics
     * @param msgPool 接收函数
     */
    public void consumer(List<String> topics, ConsumerMsgPool msgPool) {
        try {
            consumer.subscribe(topics);
            while (true) {
                ConsumerRecords<K, V> records = consumer.poll(1000);
                for (ConsumerRecord<K, V> record : records) {
                    msgPool.receiveMsg(record.value().toString());
                }
            }
        } catch (Exception e) {
            logger.error("KafkaConsumer consumering error:{}", e);
        } finally {
            consumer.close();
        }
    }


    /**
     * 消费Kafka数据
     *
     * @param topic
     * @param msgPool 接收函数
     */
    public void consumer(String topic, ConsumerMsgPool msgPool) {
        List<String> topics = new ArrayList<>();
        topics.add(topic);
        consumer(topics, msgPool);
    }


    public void consumer(int threadCount, String topic, ConsumerMsgPool msgPool) {
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        final List<MultiConsumer> consumers = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            MultiConsumer consumer = new MultiConsumer(topic, msgPool);
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


    public class MultiConsumer implements Runnable {

        private String topic;
        private ConsumerMsgPool msgPool;

        public MultiConsumer(String topic, ConsumerMsgPool msgPool) {
            this.topic = topic;
            this.msgPool = msgPool;
        }

        @Override
        public void run() {
            try {
                consumer(topic, msgPool);
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
