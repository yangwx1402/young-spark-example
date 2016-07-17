package com.young.scala.spark.example.mllib.dataype

import org.apache.spark.mllib.linalg.{Matrices, Matrix}

import scala.util.Random

/**
 * Created by Administrator on 2016/7/15.
 */
class LocalMatrixs {

  val random = new Random()

  /**
   * 密集型矩阵
   * @param row
   * @param col
   * @return
   */
  def denseLocalMatrix(row:Int,col:Int):Matrix={
    val values = Array.fill(row*col)(random.nextDouble())
    Matrices.dense(row,col,values)
  }

  /**
   * 稀疏矩阵,采用的压缩矩阵存储方式
   * @param row
   * @param col
   * @return
   */
  def sparseLocalMatrix(row:Int,col:Int):Matrix={
    Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 2, 1), Array(9, 6, 8))
  }
}
object LocalMatrixs{
  def main(args: Array[String]) {
    val localMatrixs = new LocalMatrixs
    //println(localMatrixs.denseLocalMatrix(2,3))
    println(localMatrixs.sparseLocalMatrix(2,3))
  }
}
