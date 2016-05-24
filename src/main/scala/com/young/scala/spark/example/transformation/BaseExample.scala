package com.young.scala.spark.example.transformation

import org.apache.spark.SparkContext

/**
 * Created by Administrator on 2016/5/24.
 */
trait BaseExample {

  private[example] val sparkContext = new SparkContext("local[2]","young-example")

}
