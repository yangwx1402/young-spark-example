package com.young.scala.spark.example.transformation

import java.io.File

import org.apache.spark.rdd.RDD

/**
 * Created by Administrator on 2016/5/24.
 */
object TrasformExample extends BaseExample {

  private val textFile = TrasformExample.getClass.getResource("/").getPath + File.separator + "data" + File.separator + "map.data"

  def map(): RDD[(String, Int)] = {
    val data = sparkContext.textFile(textFile).flatMap(_.split(" ")).map((_, 1))
    data
  }

  def filter(prefix: String): RDD[(String, Int)] = {
    val data = map
    val result = data.filter(_._1.startsWith(prefix))
    result
  }

  def mapPartations(partations: Int): RDD[(String, Int)] = {
    val data = sparkContext.textFile(textFile, partations).flatMap(_.split(" ")).mapPartitions(f => {
      f.map((_, 1))
    })
    data
  }

  def mapPartationsWithIndex(partations: Int): RDD[(String, Int)] = {
    val data = sparkContext.textFile(textFile, partations).flatMap(_.split(" ")).mapPartitionsWithIndex((index, f) => {
      f.map(string => (index + "_" + string, 1))
    })
    data
  }

  def sample(replaceMement:Boolean,fraction:Double,seed:Long):RDD[String]={
    val data = sparkContext.textFile(textFile)
    data.sample(replaceMement ,fraction,seed)
  }

  def main(args: Array[String]) {
    //    map().foreach(println _)
    //    filter("a").foreach(println _)
    //mapPartations(10).foreach(println _)
    //mapPartationsWithIndex(10).foreach(println _)
    sample(false,0.1,10L).foreach(println _)
  }
}
