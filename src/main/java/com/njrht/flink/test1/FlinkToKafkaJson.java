package com.njrht.flink.test1;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

public class FlinkToKafkaJson {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dStream = env.readTextFile("/usr/soft/UserBehavior.json");//"C:/Visio/UserBehavior.csv"
        env.enableCheckpointing(1000);

        FlinkKafkaProducer010<String> fkp = new FlinkKafkaProducer010<String>(
                "quickstart.cloudera:9092",//sandbox-hdp.hortonworks.co
                "UserBehavior_json",
                new SimpleStringSchema()
        );

        fkp.setWriteTimestampToKafka(true);
        dStream.addSink(fkp);

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
