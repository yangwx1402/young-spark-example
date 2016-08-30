package com.young.scala.spark.example.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Administrator on 2016/5/24.
 */
trait BaseExample {

  private[example] val sparkConf = new SparkConf()
  sparkConf.setAppName("young-example")
  sparkConf.setMaster("spark://thadoop1:7077")
  sparkConf.set("mapreduce.app-submission.cross-platform", "true")
  sparkConf.set("deploy-mode","client")

  private[example] val sparkContext = new SparkContext(sparkConf)

  sparkContext.setLogLevel("DEBUG")

}
