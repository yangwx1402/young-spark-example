package com.young.scala.spark.example.mllib.cf

import java.io.File

import com.young.scala.spark.example.transformation.BaseExample
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating, ALS}
import org.apache.spark.rdd.RDD

/**
 * Created by Administrator on 2016/7/15.
 */
class CFRecommendTools {

  private var model: MatrixFactorizationModel = null

  /**
   *
   * @param trainData  训练数据
   * @param rank  特征数,也就是用户特征向量的长度
   * @param iteraNum  迭代次数
   * @param lambda   规范化系数,建议为0.01
   * @param block  并行计算分区数,-1为自动选择
   */
  def trainModel(trainData: RDD[Rating], rank: Int, iteraNum: Int = 10, lambda: Double = 0.01, block: Int = (-1)) {
    model = ALS.train(trainData, rank, iteraNum, lambda, block)
  }

  def getModel: MatrixFactorizationModel = if (model == null) throw new Exception("you must training model first") else model

  def predict(userId: Int, itemId: Int) = getModel.predict(userId, itemId)

  def predict(userItems: RDD[(Int, Int)]): RDD[Rating] = getModel.predict(userItems)

  def trasferData(ratings: RDD[Rating]): RDD[(Int, Int)] = ratings.map(rating => (rating.user, rating.product))

  def mse(testData: RDD[Rating]): Double = {
    val trasferTestData = trasferData(testData)
    val orignData = testData.map(rating => ((rating.user, rating.product), rating.rating))
    predict(trasferTestData).foreach(println _)
        predict(trasferTestData).map(rating => ((rating.user, rating.product), rating.rating)).join(orignData)
          .map(f => {
            val err = f._2._1 - f._2._2
            err * err
          }).mean()
  }

}

object CFRecommendTools extends BaseExample {

  def getData(dataPath: String, trasfer: (String => Rating), sc: SparkContext): RDD[Rating] = {
    sc.textFile(dataPath,4).map(line => trasfer(line))
  }

  def main(args: Array[String]) {
    val tools = new CFRecommendTools
    val dataPath = CFRecommendTools.getClass.getResource("/").getPath + File.separator + "data" + File.separator + "grouplens" + File.separator + "1m" + File.separator + "ratings.dat"
    def trasferFunction(line: String): Rating = {
      val array = line.split("::")
      Rating(array(0).toInt, array(1).toInt, array(2).toDouble)
    }
    val data = CFRecommendTools.getData(dataPath, trasferFunction, sparkContext)
    val splitData = data.randomSplit(Array(0.8, 0.2))
    val trainData = splitData(0)
    val testData = splitData(1)
    tools.trainModel(trainData, 50, 20)
    val trasferData = tools.trasferData(testData)
    tools.predict(trasferData).foreach(println _)
    println(tools.mse(testData))
    //1.4887318007957853
  }
}
