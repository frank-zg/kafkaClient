package com.simple.kafka.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by frank on 2016/11/3.
 *
 * @apiNote Simple KafkaProducer
 * @apiNote kafka-client-0.10.0.1
 */
public class KafkaProducer<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);

    private Producer<K, V> producer;
    private Properties properties = new Properties();


    /**
     * 初始化默认配置
     */
    private void initProperties() {
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("client.id", "testClient");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("acks", "1");
        properties.put("retries", "0");
        properties.put("linger.ms", "500");
        properties.put("auto.create.topics.enable", true);
    }


    /**
     * 初始化KafkaProducer
     *
     * @param props default config:
     *              <p>
     *              bootstrap.servers", "localhost:9092" 服务器列表</br>
     *              client.id", "testClient"   clientID</br>
     *              key.serializer", "org.apache.kafka.common.serialization.StringSerializer"   key序列化格式</br>
     *              value.serializer", "org.apache.kafka.common.serialization.StringSerializer" value序列化格式</br>
     *              acks", "1"  commit方式</br>
     *              retries", "3"   重试次数</br>
     *              linger.ms", "500"</br>
     *              auto.create.topics.enable", true</br>
     */
    public KafkaProducer(Properties props) {
        initProperties();
        if (props == null) {
            props = this.properties;
        }
        try {
            producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
        } catch (Exception e) {
            logger.error("kafka producer init error:{}", e);
        }
    }


    /**
     * 往kafka发送消息
     *
     * @param topic    topic
     * @param msg      发送的数据
     * @param callBack callback
     */
    public void sendMsg(String topic, V msg, ProducerCallBack callBack) {
        producer.send(new ProducerRecord<>(topic, msg), callBack);
    }


    /**
     * 往kafka发送消息
     *
     * @param topic topic
     * @param msg   发送的数据
     */
    public void sendMsg(String topic, V msg) {
        producer.send(new ProducerRecord<>(topic, msg));
    }

}
