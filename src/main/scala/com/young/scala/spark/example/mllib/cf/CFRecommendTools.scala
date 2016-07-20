package com.young.scala.spark.example.mllib.cf

import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating, ALS}
import org.apache.spark.rdd.RDD

/**
 * Created by Administrator on 2016/7/15.
 */
class CFRecommendTools {

  private var model:MatrixFactorizationModel = null

  def trainModel(trainData:RDD[Rating],rank:Int,iteraNum:Int=10,lambda:Double=0.01,block:Int=(-1)){
     model = ALS.train(trainData,rank,iteraNum,lambda,block)
  }

  def getModel:MatrixFactorizationModel=if (model==null) throw new Exception("you must training model first") else model

  def predict(userId:Int,itemId:Int)= getModel.predict(userId,itemId)

  def predict(userItems:RDD[(Int,Int)]):RDD[Rating]=getModel.predict(userItems)

  def mse(testData:RDD[Rating]):Double={
    val trasferTestData = testData.map(rating=>(rating.user,rating.product))
    val orignData = testData.map(rating=>((rating.user,rating.product),rating.rating))
    predict(trasferTestData).map(rating=>((rating.user,rating.product),rating.rating)).join(orignData)
   .map(f=>{
      val err = f._2._1-f._2._2
     err*err
   }).mean()
  }

}
object CFRecommendTools{

  def getData(dataPath:String,trasfer:(String=>Rating),sc:SparkContext):RDD[Rating]={
      sc.textFile(dataPath).map(line=>trasfer(line))
  }

  def main(args: Array[String]) {

  }
}
