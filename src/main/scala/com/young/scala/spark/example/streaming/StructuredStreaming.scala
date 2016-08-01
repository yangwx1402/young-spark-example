package com.young.scala.spark.example.streaming
import org.apache.spark.sql.SparkSession

/**
 * Created by dell on 2016/8/1.
 */
object StructuredStreaming extends BaseDataFrame{
  def worcount(session:SparkSession): Unit ={
    import session.implicits._
    val lines = session.readStream.format("socket").option("host","").option("port","9999").load("E:\\data")
    val words = lines.as[String].flatMap(_.split(" "))
    val wordcount = words.groupBy("value").count()
    wordcount.printSchema()

  }
  def main(args: Array[String]) {
    StructuredStreaming.worcount(sparkSession)
  }
}
