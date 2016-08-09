package com.young.scala.spark.example.rdd

import org.apache.spark.{TaskContext, Partition}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.{RDD}

/**
 * Created by Administrator on 2016/8/3.
 */
case class User(username:String,age:Int)
class UserRDD(prev:RDD[User]) extends RDD[User](prev){
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[User] = ???

  override protected def getPartitions: Array[Partition] = ???
}
