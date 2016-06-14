package com.young.scala.spark.example.transformation

import java.io.File

import org.apache.spark.rdd.RDD

/**
 * Created by Administrator on 2016/5/24.
 */
object TrasformExample extends BaseExample {

  private val textFile = TrasformExample.getClass.getResource("/").getPath + File.separator + "data" + File.separator + "map.data"

  private val map2File = TrasformExample.getClass.getResource("/").getPath + File.separator + "data" + File.separator + "map2.data"

  private val distinctFile = TrasformExample.getClass.getResource("/").getPath + File.separator + "data" + File.separator + "distinct.data"

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

  def sample(replaceMement: Boolean, fraction: Double, seed: Long): RDD[String] = {
    val data = sparkContext.textFile(textFile)
    data.sample(replaceMement, fraction, seed)
  }

  def union(data1: RDD[String], data2: RDD[String]): RDD[String] = {
    data1.union(data2)
  }

  //交集
  def intersection(data1: RDD[String], data2: RDD[String]): RDD[String] = {
    data1.intersection(data2)
  }

  def distinct(data: RDD[String], partation: Int): RDD[String] = {
    data.distinct(partation)
  }

  def groupBykey(): RDD[(String, Int)] = {
    val data = sparkContext.textFile(textFile, 2).flatMap(_.split(" ")).map((_, 1)).groupByKey().map(f => {
      (f._1, f._2.reduce(_ + _))
    })
    data
  }

  def main(args: Array[String]) {
    //    map().foreach(println _)
    //    filter("a").foreach(println _)
    //mapPartations(10).foreach(println _)
    //mapPartationsWithIndex(10).foreach(println _)
    //sample(false,0.1,10L).foreach(println _)
    val data1 = sparkContext.textFile(textFile)
    val data2 = sparkContext.textFile(map2File)
    //val data = union(data1,data2)
    //val data = intersection(data1,data2)
    //val data = distinct(data1,2)
    val distinctData = sparkContext.textFile(distinctFile, 2)
    //val data = distinct(distinctData,2)
    val data = groupBykey()
    data.foreach(println _)
  }
}
