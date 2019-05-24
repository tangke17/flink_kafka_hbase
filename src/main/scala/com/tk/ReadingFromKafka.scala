package com.tk

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09

object ReadingFromKafka {
  private val ZOOKEEPER_HOST="192.168.12.248:2181"
  private val KAFKA_BROKER="192.168.12.248:9092"
  private val TRANSACTION_GROUP="test-consumer-group"
  private val TOPIC="test2"

  def main(args: Array[String]): Unit = {

    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    val kafkaProps=new Properties()
    kafkaProps.setProperty("zookeeper.connect",ZOOKEEPER_HOST)
    kafkaProps.setProperty("bootstrap.servers",KAFKA_BROKER)
    kafkaProps.setProperty("group.id",TRANSACTION_GROUP)

    var stream=env
      .addSource(
        new FlinkKafkaConsumer09(TOPIC,new SimpleStringSchema(),kafkaProps)
      )


    val s:DataStream[String]=stream.map(x=>(x+"xxxxx"))
    //println(s)
    s.print()
    //stream.print()
    s.addSink(new SinkData2phoenix)
    //stream.setParallelism(4).writeAsText("hdfs:///tmp/iteblog/data")
    env.execute()
  }

}
