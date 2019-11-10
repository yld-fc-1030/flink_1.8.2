package com.njrht.flink.test1;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;
import org.apache.kafka.common.config.ConfigDef;

public class KafkaResourceJson {
    private static final TypeInformation<Row> SCHEMA = Types.ROW(
            new String[]{"userId", "categoryId", "itemId", "behavior", "timestamp"},
            new TypeInformation[]{
                    Types.LONG(),
                    Types.LONG(),
                    Types.INT(),
                    Types.STRING(),
                    Types.SQL_TIMESTAMP()
            }
    );
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);//引入event-time使劲按属性
        env.enableCheckpointing(5000);//checkpingting
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.connect(
                new Kafka()
                        .version("0.10")
                        .topic("UserBehavior_json")
                        .startFromEarliest()
                        .property("zookeeper.connect","quickstart.cloudera:2181")//quickstart.cloudera
                        .property("bootstrap.servers","quickstart.cloudera:9092")
                        .startFromEarliest()//从最早的数据进行消费，忽略存储的offset信息
                        .sinkPartitionerFixed()//每个flink分区最多在一个kafka分区中结束
        )
                .withFormat(
                        new Json()
                        .failOnMissingField(true)//标记字段丢失是否fail
                        .schema(Types.ROW(SCHEMA))
                )
                .withSchema(new Schema()
                        .field("userId", Types.LONG())
                        .field("categoryId", Types.LONG())
                        .field("itemId", Types.INT())
                        .field("behavior", Types.STRING())
                        .field("timestamp",Types.LONG())
                        .field("rowtime", Types.SQL_TIMESTAMP())
                        .rowtime(new Rowtime()
                                .timestampsFromField("timestamp")
                                .watermarksPeriodicBounded(60000)
                        )
                )
                .inAppendMode()
                .registerTableSource("User_Json_tab");
        Table table = tableEnv.scan("User_Json_tab");
        table.printSchema();

        FlinkKafkaProducer010 myProducer = new FlinkKafkaProducer010<String>(
                "quickstart.cloudera:9092",         // broker list
                "test1",               // target topic
                new SimpleStringSchema()
        );   // serialization schema
        myProducer.setWriteTimestampToKafka(true);

        tableEnv.toAppendStream(table, Row.class).addSink(myProducer).setParallelism(1);
        tableEnv.toAppendStream(table, Row.class).print();
        try {
            env.execute("Us");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
//                                "{" +
//                                        "   type: 'object'," +
//                                        "   properties: {" +
//                                        "       userId: {" +
//                                        "           type: 'number'" +
//                                        "       }," +
//                                        "       itemId: {" +
//                                        "           type: 'number'" +
//                                        "       }," +
//                                        "       categoryId: {" +
//                                        "           type: 'number'" +
//                                        "       }," +
//                                        "       behavior: {" +
//                                        "           type: 'string'" +
//                                        "       }," +
//                                        "       timestamp: {" +
//                                        "           type: 'string'" +
//                                        "           format: 'date-time'" +
//                                        "       }" +
//                                        "   }" +
//                                        "}"