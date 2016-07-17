package com.young.scala.spark.example.mllib.dataype

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import scala.util.Random

/**
 * Created by young on 2016/7/15.
 * local Vector其实就是一个向量,不过是存储在单机内存中的,其中分为两种存储类型
 * 一个为密集型的Vector两一个为稀疏型的Vector,密集型意思就是说每个下标都有值,
 * 而稀疏型并不是每个下标都有值
 */
class LocalVector {

  val random = new Random(1)

  /**
   * 随机产生一个固定维度的密集型向量
   * @param num
   * @return
   */
  def denseVector(num:Int):Vector={
    Vectors.dense(Array.fill(num)(random.nextDouble()))
  }

  /**
   * 随机产生一个维度固定的稀疏型向量
   * @param num
   * @return
   */
  def sparseVector(num:Int):Vector={
    val values = Array.fill(num)(random.nextDouble())
    val index = (0 until num).toArray
    Vectors.sparse(num,index,values)
  }
}
object LocalVector{
  def main(args: Array[String]) {
    val localVector = new LocalVector
    val num = 5
    println(localVector.denseVector(num))
    println(localVector.sparseVector(num))
  }
}
