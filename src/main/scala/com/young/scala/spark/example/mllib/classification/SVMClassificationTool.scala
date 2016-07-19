package com.young.scala.spark.example.mllib.classification

import java.io.File

import com.young.scala.spark.example.transformation.BaseExample
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

/**
 * Created by dell on 2016/7/19.
 */
class SVMClassificationTool extends Serializable{

  private var model: SVMModel = null


  def trainModel(trainingData: RDD[LabeledPoint], numIterations: Int = 100) {
    /**
     * 这里可以针对SVMWithSGD的模型训练参数进行设置
     * val svmAlg = new SVMWithSGD()
       svmAlg.optimizer.
       setNumIterations(200).
       setRegParam(0.1).
       setUpdater(new L1Updater)
       val modelL1 = svmAlg.run(training)
     */
    model = SVMWithSGD.train(trainingData, numIterations)
  }

  def getModel(): SVMModel = if (model == null) throw new Exception("model is not train,please call trainModel method first") else model

  def predict(labeledPoint: LabeledPoint): Double = getModel().predict(labeledPoint.features)

  def saveModel(modelPath: String, sparkContext: SparkContext) = model.save(sparkContext, modelPath)

  def loadModel(modelPath: String, sparkContext: SparkContext) = model = SVMModel.load(sparkContext, modelPath)

  def getModelMetrics(testData: RDD[LabeledPoint]): Double = {
    val scoreAndLabels = testData.map { point =>
      val score = model.predict(point.features)
      println(score+":"+point.label)
      (score, point.label)
    }
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    metrics.areaUnderROC()
  }
}

object SVMClassificationTool extends BaseExample{
  def getSVMData(dataPath: String, sparkContext: SparkContext): RDD[LabeledPoint] = {
    MLUtils.loadLibSVMFile(sparkContext, dataPath)
  }

  def main(args: Array[String]) {
    val dataPath = SVMClassificationTool.getClass.getResource("/").getPath + File.separator + "data" + File.separator + "svm" + File.separator + "sample_libsvm_data.txt"
    val data = getSVMData(dataPath,sparkContext)
    val tool = new SVMClassificationTool
    val splitData = data.randomSplit(Array(0.7,0.3))
    val trainingData = splitData(0)
    val testData = splitData(1)
    tool.trainModel(trainingData)
    print(tool.getModelMetrics(testData))

  }
}
