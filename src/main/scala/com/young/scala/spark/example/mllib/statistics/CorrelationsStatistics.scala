package com.young.scala.spark.example.mllib.statistics

import com.young.scala.spark.example.mllib.dataype.DistributedMatrixs
import org.apache.spark.mllib.linalg.{Matrix, Vector}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD

import scala.util.Random

/**
 * Created by dell on 2016/7/18.
 * 统计相关的算法
 */
class CorrelationsStatistics {

  /**
   * 计算两个向量之间的距离
   * @param vector1
   * @param vector2
   * @param method
   * @return
   */
  def distanceForVector(vector1: RDD[Double], vector2: RDD[Double], method: String = "pearson"): Double = {
    Statistics.corr(vector1, vector2, method)
  }

  /**
   * 计算整个矩阵的距离,迭代计算,每个向量都会计算与其他向量之间的距离,最后返回结果是距离矩阵
   * 其他的距离算法还有spearman
   * @param matrix
   * @param method
   * @return
   */
  def distanceForMatrix(matrix: RDD[Vector], method: String = "pearson"): Matrix = {
    Statistics.corr(matrix, method)
  }
}

object CorrelationsStatistics {
  def main(args: Array[String]) {
    val test = new CorrelationsStatistics
    val random = new Random
    val tool = new DistributedMatrixs
    val dime = 10
    val seq1 = Array.fill(dime)(random.nextDouble()).toSeq
    val seq2 = Array.fill(dime)(random.nextDouble()).toSeq
    val vector1 = tool.sparkContext.parallelize(seq1)
    val vector2 = tool.sparkContext.parallelize(seq2)
    println(test.distanceForVector(vector1, vector2,"spearman"))
    val matrix = tool.getRowMatrix(10,10)
    val distances = test.distanceForMatrix(matrix.rows)
    println(distances)
  }
}
