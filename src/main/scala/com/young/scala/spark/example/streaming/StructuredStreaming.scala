package com.young.scala.spark.example.streaming
import org.apache.spark.sql.SparkSession


/**
 * Created by dell on 2016/8/1.
 */
class StructuredStreaming {

  def worcount(session:SparkSession): Unit ={
    val lines = session.readStream.format("socket").option("host","")
  }
}
