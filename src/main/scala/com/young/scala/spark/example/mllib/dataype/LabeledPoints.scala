package com.young.scala.spark.example.mllib.dataype

import org.apache.spark.mllib.regression.LabeledPoint

/**
 * Created by Administrator on 2016/7/15.
 * LabeledPoint无非就是在LocalVector的基础上增加了一个Label,主要用于分类等场景,
 * 例如分类结果
 */

class LabeledPoints {

     def lebeledPoints():LabeledPoint={
       val localVector = new LocalVector
       val denseVector = localVector.denseVector(10)
       LabeledPoint(1.0,denseVector)
     }
}

object LabeledPoints{
  def main(args: Array[String]) {
    val labeledPoints = new LabeledPoints
    println(labeledPoints.lebeledPoints())
  }
}
