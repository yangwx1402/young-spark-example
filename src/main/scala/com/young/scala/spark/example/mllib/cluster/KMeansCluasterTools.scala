package com.young.scala.spark.example.mllib.cluster

import java.io.File

import com.young.scala.spark.example.transformation.BaseExample
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vectors, Vector}

/**
 * Created by Administrator on 2016/7/15.
 */
class KMeansCluasterTools {

  /**
   *
   * @param trainData 训练数据
   * @param k   k
   * @param maxIterations 最大迭代次数
   * @param runs   聚类次数,运行多次可以提高效果
   */
  def train(trainData: RDD[Vector], k: Int, maxIterations: Int = 10, runs: Int = 1): KMeansModel = {
    KMeans.train(trainData, k, maxIterations, runs)
  }

  /**
   * 计算距离最近的中心点的平方和,通过该指标来评价聚类算法
   * @param data
   * @param model
   * @return
   */
  def wssse(data: RDD[Vector], model: KMeansModel): Double = model.computeCost(data)


  /**
   * 根据中心点对每个向量数据进行归类
   * @param model
   * @param testData
   * @return
   */
  def classify(model: KMeansModel, testData: RDD[(Int, Vector)]): RDD[(Int, Int)] = {
    testData.map(line => {
      (line._1, model.predict(line._2))
    })
  }
}

object KMeansCluasterTools extends BaseExample {
  /**
   * 获取聚类数据
   * @param dataPath
   * @param trasferFunction
   * @param sc
   * @return
   */
  def getData(dataPath: String, trasferFunction: (String => Vector), sc: SparkContext): RDD[Vector] = {
    sc.textFile(dataPath).map(trasferFunction(_))
  }

  def main(args: Array[String]) {
    val dataPath = KMeansCluasterTools.getClass.getResource("/").getPath + File.separator + "data" + File.separator + "grouplens" + File.separator + "1m" + File.separator + "ratings.dat"
    val tools = new KMeansCluasterTools
    val data = sparkContext.textFile(dataPath).map(line => {
      val temp = line.split("::")
      (temp(0).toInt, (temp(1).toInt, temp(2).toDouble))
    }).groupByKey().map(f => {
      (f._1,Vectors.sparse(3953, f._2.toSeq))
    })
    /**
     * 对数据进行抽样
     */
    val splitData = data.randomSplit(Array(0.7,0.3))
    val trainData = splitData(0).map(f=>f._2)
    val testData = splitData(1)
    val model = tools.train(trainData, 10, 10, 2)
    model.clusterCenters.foreach(println _)
    println(tools.wssse(trainData, model))
    tools.classify(model,testData).foreach(println _)
  }
}

