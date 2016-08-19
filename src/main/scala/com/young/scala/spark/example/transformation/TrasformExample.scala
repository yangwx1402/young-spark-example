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

  /**
   * repartition(numPartitions:Int):RDD[T]和coalesce(numPartitions:Int，shuffle:Boolean=false):RDD[T]

他们两个都是RDD的分区进行重新划分，repartition只是coalesce接口中shuffle为true的简易实现，（假设RDD有N个分区，需要重新划分成M个分区）

1）、N<M。一般情况下N个分区有数据分布不均匀的状况，利用HashPartitioner函数将数据重新分区为M个，这时需要将shuffle设置为true。

2）如果N>M并且N和M相差不多，(假如N是1000，M是100)那么就可以将N个分区中的若干个分区合并成一个新的分区，最终合并为M个分区，这时可以将shuff设置为false，在shuffl为false的情况下，如果M>N时，coalesce为无效的，不进行shuffle过程，父RDD和子RDD之间是窄依赖关系。

3）如果N>M并且两者相差悬殊，这时如果将shuffle设置为false，父子ＲＤＤ是窄依赖关系，他们同处在一个Ｓｔａｇｅ中，就可能造成spark程序的并行度不够，从而影响性能，如果在M为1的时候，为了使coalesce之前的操作有更好的并行度，可以讲shuffle设置为true。

总之：如果shuff为false时，如果传入的参数大于现有的分区数目，RDD的分区数不变，也就是说不经过shuffle，是无法将RDDde分区数变多的。
   * @return
   */
  def coalesce(): RDD[(String, Int)] = {
    val data = sparkContext.textFile(textFile, 2).flatMap(_.split(" ")).map((_, 1)).filter(f => f._1.size < 10).coalesce(2)
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
