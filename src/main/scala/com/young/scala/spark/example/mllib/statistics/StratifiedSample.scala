package com.young.scala.spark.example.mllib.statistics

import com.young.scala.spark.example.transformation.BaseExample
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD

import scala.util.Random

/**
 * Created by Administrator on 2016/7/18.
 * spark mllib抽样学习
 */
class StratifiedSample extends BaseExample{

  private val random = new Random

   def generateData(rowNum:Int,colNum:Int,yu:Int=2):RDD[(Long,Vector)]={
     var num = 0l
     val seq = Array.fill(rowNum)({
       num+=1
       (num%yu,{
         Vectors.dense(Array.fill(colNum)(random.nextDouble()))
       })
     }).toSeq
     sparkContext.parallelize(seq)
   }
}
object StratifiedSample{
  def main(args: Array[String]) {
    val example = new StratifiedSample
    val data = example.generateData(100,10)

    /**
     * 抽樣的時候,每個Key都必须有对应的比例,否则会报错
     */
    val factor = Map(1L->0.5,0L->0.5)
    val sample1 = data.sampleByKey(false,factor)
    sample1.foreach(println _)
    val sample2 = data.sampleByKeyExact(false,factor)
    sample2.foreach(println _)
  }

}
