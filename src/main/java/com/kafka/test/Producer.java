package com.kafka.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Producer {
    public static void main(String[] args) {
        // TODO 创建配置
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // TODO 对生产的数据做K,V序列化
        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // TODO 创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configMap);

        // TODO 创建数据record, 需要传递三个参数：指定的topic，数据Key，数据Value
//        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
//                "test", "key", "value"
//        );

        // TODO 通过生产者对象将数据发送到kafka服务器
//        producer.send(record);

        for (int i = 0; i < 10; i++) {
            // TODO 创建数据record, 需要传递三个参数：指定的topic，数据Key，数据Value
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(
                    "test", "key"+i, "value"+i
            );
            // TODO 通过生产者对象将数据发送到kafka服务器
            producer.send(record);
        }

        // TODO 关闭生产者对象
        producer.close();
    }
}
