package com.young.scala.spark.example.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext, Seconds}

/**
 * Created by dell on 2016/8/1.
 */
object StreamingStudy {
  def wordcount(conf:SparkConf,seconds:Int): Unit ={
    val ssc = new StreamingContext(conf, Seconds(seconds))
    val lines = ssc.socketTextStream("120.25.69.139",9999)
    val words = lines.flatMap(_.split(" ")).map((_,1))
    val wordcount = words.reduceByKey(_+_)
    wordcount.print()
    ssc.start()
    ssc.awaitTermination()
  }
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("young-example")

    /**
     * 单机的时候local[2] 必须>1,因为接受和处理数据再不同的线程中
     */
    sparkConf.setMaster("local[2]")
    StreamingStudy.wordcount(sparkConf,5)
  }
}
