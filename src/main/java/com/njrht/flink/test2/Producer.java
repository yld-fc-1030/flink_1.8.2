package com.njrht.flink.test2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class Producer {
    public static void main(String[] args){
        int events = 100;
        Properties props = new Properties();
        props.put("bootstrap.servers"
                , "168.61.2.47:9092," +
                        "168.61.2.48:9092," +
                        "168.61.2.49:9092," +
                        "168.61.11.161:9092," +
                        "168.61.11.162:9092," +
                        "168.61.11.163:9092");
        props.put("acks", "1");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 16384);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //创建生产者
        KafkaProducer producer;
        try{
            producer = new KafkaProducer(props);
            for (int i = 0; i < events; i++){
                try {
                    System.out.println("send :  " + i);
                    producer.send(new ProducerRecord<>("test_WSB", Integer.toString(i), Integer.toString(i)));
                    Thread.sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            producer.close();

        }catch (Exception e){
            e.printStackTrace();
        }
    }
}