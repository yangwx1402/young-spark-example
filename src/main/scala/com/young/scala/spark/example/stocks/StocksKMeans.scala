package com.young.scala.spark.example.stocks

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by dell on 2016/9/30.
 */
object StocksKMeans {

  def training(traingData: RDD[Vector], k: Int, numIterations: Int): KMeansModel = {
    KMeans.train(traingData, k, numIterations)
  }

  def classifier(model: KMeansModel, data: RDD[(String, Vector)]): Array[(Int, Array[String])] = {
    data.map(feature => {
      (model.predict(feature._2), feature._1)
    }).groupByKey().map(x => (x._1, x._2.toArray)).collect()
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("young-example")
    sparkConf.setMaster("local[2]")
    val sparkContext = new SparkContext(sparkConf)
    val dataPath = "E:\\project\\young\\scala\\young-spark-example\\src\\main\\resources\\data\\stocks\\transfer\\last_300.data"
    val data = sparkContext.textFile(dataPath).map(line => {
      val array = line.split(",")
      val code = array(0)
      val price = array(7)
      if (price.toDouble > 0)
        (code, 1.0)
      else
        (code, 0.0)
    }).groupByKey.map(x => (x._1, Vectors.dense(x._2.toArray)))
    val traingData = data.map(_._2)
    val model = training(traingData,10,20)
    val result = classifier(model,data)
    result.foreach(rs=>println(rs._1+","+rs._2.mkString(",")))
  }
}
