package com.young.scala.spark.example.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Administrator on 2016/5/24.
 */
trait BaseExample {

  private[example] val sparkConf = new SparkConf()
  sparkConf.setAppName("young-example")
  sparkConf.setMaster("local[2]")

  private[example] val sparkContext = new SparkContext(sparkConf)

  sparkContext.setLogLevel("INFO")

}
