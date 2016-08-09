package com.young.scala.spark.example.streaming

import org.apache.spark.SparkConf

/**
 * Created by dell on 2016/8/1.
 */
trait BaseDataFrame {

  private[example] val sparkConf = new SparkConf()
  sparkConf.setAppName("young-example")
  sparkConf.setMaster("local[2]")

}
