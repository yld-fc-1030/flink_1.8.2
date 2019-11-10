package test1

import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
object CsvToKafka {

  def tranTimeToString(tm:String) :String={
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val tim = fm.format(new Date(tm.toLong))
    tim
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dStream:DataStream[String] = env.readTextFile("/usr/soft/UserBehavior.csv")
    //创建Sink端(Kafka)
    val myProducer = new FlinkKafkaProducer010[String](
      "quickstart.cloudera:9092",         // broker list
      "UserBehavior_time",               // target topic
      new SimpleStringSchema)   // serialization schema

    // versions 0.10+ allow attaching the records' event timestamp when writing them to Kafka;
    // this method is not available for earlier Kafka versions
    myProducer.setWriteTimestampToKafka(false)//允许写入kafka得时间戳
    val ds = dStream.map(a =>{
      var b = a.split(",")
      b(0)+","+b(1)+","+b(2)+","+b(3)+","+tranTimeToString(b(4))
    })
    ds.print()
    ds.addSink(myProducer)	//输出到kafka

    env.execute()	//开始执行
  }
}
