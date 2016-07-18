package com.young.scala.spark.example.mllib.statistics

import com.young.scala.spark.example.mllib.dataype.{DistributedMatrixs, LocalMatrixs, LocalVector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, Vector}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD

/**
 * Created by dell on 2016/7/18.
 * 基础算法
 */

class SummaryStatistics {

  /**
   * 均值
   * @param vectors
   * @return
   */
  def mean(vectors: RDD[Vector]): Vector = {
    Statistics.colStats(vectors).mean
  }

  /**
   * 方差
   * @param vectors
   * @return
   */
  def variance(vectors: RDD[Vector]): Vector = {
    Statistics.colStats(vectors).variance
  }

  /**
   * 统计每一列的非零个数
   * @param vectors
   * @return
   */
  def numNonzeros(vectors: RDD[Vector]): Vector = {
    Statistics.colStats(vectors).numNonzeros
  }

  /**
   * 返回样本个数
   * @param vectors
   * @return
   */
  def count(vectors: RDD[Vector]): Long = {
    Statistics.colStats(vectors).count
  }

  /**
   * 返回每列中最大的元素
   * @param vectors
   * @return
   */
  def max(vectors: RDD[Vector]): Vector = {
    Statistics.colStats(vectors).max
  }

  def min(vectors:RDD[Vector]):Vector={
    Statistics.colStats(vectors).min
  }

  /**
   * L1 norm就是绝对值相加，又称曼哈顿距离
   * 算法就是将矩阵按照index取绝对值想加
   * @param vectors
   * @return
   */
  def normL1(vectors:RDD[Vector]):Vector={
    Statistics.colStats(vectors).normL1
  }

  /**
   * L2 norm就是欧几里德距离
   * @param vectors
   * @return
   */
  def normL2(vectors:RDD[Vector]):Vector={
    Statistics.colStats(vectors).normL2
  }
}

object SummaryStatistics {
  def main(args: Array[String]) {
    val test = new SummaryStatistics
    val local = new DistributedMatrixs
    val matrix: RowMatrix = local.getRowMatrix(10, 10)
    val vectors = matrix.rows
    vectors.foreach(println _)
    println(test.mean(vectors))
    println(test.variance(vectors))
    println(test.numNonzeros(vectors))
    println(test.count(vectors))
    println(test.max(vectors))
    println(test.min(vectors))
    println(test.normL1(vectors))
    print(test.normL2(vectors))
  }
}
