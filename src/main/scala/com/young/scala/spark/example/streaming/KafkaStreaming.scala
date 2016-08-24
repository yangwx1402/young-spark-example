package com.young.scala.spark.example.streaming

import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization._
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
 * Created by dell on 2016/8/8.
 */
object KafkaStreaming extends BaseDataFrame{

     def processKafkaMessage(streamingContext: StreamingContext,zk:String="localhost:2181",groupId:String="test",topicAndPartitaions:Map[String,Int])={
       val kafkaParams = Map[String, String](
      "zookeeper.connect" -> zk, "group.id" -> groupId,
      "zookeeper.connection.timeout.ms" -> "10000")
       val kafkaStreaming  = KafkaUtils.createStream[Int,String,IntDecoder,StringDecoder](streamingContext,kafkaParams,topicAndPartitaions,StorageLevel.MEMORY_AND_DISK_SER_2)
       kafkaStreaming.foreachRDD(_.foreach(println _))
       streamingContext.start()
       streamingContext.awaitTermination()
     }

  def main(args: Array[String]) {
    val sparkContext = new SparkContext(sparkConf)
    val streamingContext = new StreamingContext(sparkContext,Duration(10000))
    val map = Map[String,Int]("test"->2)
    println(map)
    val zk = "115.29.47.216:2181"
    KafkaStreaming.processKafkaMessage(streamingContext,zk,"test",map)
  }

}
