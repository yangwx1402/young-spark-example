package com.young.scala.spark.example.kafka

import java.io.{FileInputStream, File}
import java.util.Properties

import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}

/**
 * Created by Administrator on 2016/8/9.
 */
object KafkaTools {

  val configPath = KafkaTools.getClass.getResource("/").getPath+File.separator+"kafka"+File.separator+"producer.properties"

  val config = new Properties()
  config.load(new FileInputStream(configPath))

  val producer = new KafkaProducer[Integer,String](config)

  def productMessage(num:Int,topic:String): Unit ={
    for(i<-0 to num) {
      producer.send(new ProducerRecord(topic, i, "message_" + i))
    }
    producer.close()
  }

  def main(args: Array[String]) {
    KafkaTools.productMessage(100,"test")
  }
}
