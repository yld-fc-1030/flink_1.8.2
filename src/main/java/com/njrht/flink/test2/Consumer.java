package com.njrht.flink.test2;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {

        Properties props = new Properties();
        /* 定义kakfa 服务的地址，不需要将所有broker指定上 */
        props.put("bootstrap.servers", "168.61.2.47:9092," +
                "168.61.2.48:9092," +
                "168.61.2.49:9092," +
                "168.61.11.161:9092," +
                "168.61.11.162:9092," +
                "168.61.11.163:9092");
        /* 制定consumer group */
        props.put("group.id", "User_Csv_WSB");
        /* 是否自动确认offset */
        props.put("enable.auto.commit", "true");
        /* 自动确认offset的时间间隔 */
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        /* key的序列化类 */
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        /* value的序列化类 */
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        /* 定义consumer */
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        /* 消费者订阅的topic, 可同时订阅多个 */
        consumer.subscribe(Arrays.asList("User_Csv_WSB"));

        /* 读取数据，读取超时时间为100ms */
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {

                System.out.printf("offset = %d, key = %s, value = %s",
                        record.offset(),
                        record.key(),
                        record.value());
            }

        }
    }
}
