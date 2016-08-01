package com.young.scala.spark.example.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by dell on 2016/8/1.
 */
trait BaseDataFrame {

  private[example] val sparkConf = new SparkConf()
  sparkConf.setAppName("young-example")
  sparkConf.setMaster("local[2]")

  val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
}
