package com.young.scala.spark.example.accumulator

import java.io.File

import com.young.scala.spark.example.transformation.BaseExample

/**
 * Created by dell on 2016/8/24.
 */
object AccumulatorExample extends BaseExample {

  def main(args: Array[String]) {
    val dataPath = AccumulatorExample.getClass.getResource("/").getPath + File.separator + "data" + File.separator + "grouplens" + File.separator + "10k" + File.separator + "u1.base"
    val dataRdd = sparkContext.textFile(dataPath, 2)
    val accum = sparkContext.accumulator(0)
    dataRdd.foreach(x => {
      accum.add(1)
    })
    print(accum.value)
  }
}
