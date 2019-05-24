package com.tk

import java.util

import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import java.util
import java.util.Properties
import org.apache.flink.configuration.Configuration
import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

class sourceConsumer extends RichSourceFunction[String]{
  var consumer: KafkaConsumer[String,String]=null
  var props:Properties= new Properties
  val TOPIC = "test2"
  override def open(parameters: Configuration): Unit = {
    println("1111111111111111")
     //props.put("bootstrap.servers", "192.168.12.248:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "test")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "6000")
    props.put("bootstrap.servers","192.168.12.248:9092")

  }
  override def run(sourceContext: SourceContext[String]):Unit={
    consumer=new KafkaConsumer[String,String](props)
    consumer.subscribe(util.Collections.singletonList(TOPIC))
    try{
        while(true) {
          val records: ConsumerRecords[String,String] = consumer.poll(100)
          for (record <- records.asScala) {
            println(record)
            sourceContext.collect(record.key()+"|"+record.value())
          }
        }

    } catch {
      case e => e.printStackTrace()
    }
  }

  override def cancel(): Unit = {
  }
}
