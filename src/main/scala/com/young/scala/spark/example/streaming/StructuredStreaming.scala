package com.young.scala.spark.example.streaming
import org.apache.spark.sql.SparkSession

/**
 * Created by dell on 2016/8/1.
 */
object StructuredStreaming extends BaseDataFrame{
  def worcount(session:SparkSession): Unit ={
    import session.implicits._
    val lines = session.readStream.format("socket").option("host","120.25.69.139").option("port","9999").load()
    val words = lines.as[String].flatMap(_.split(" "))
    val wordcount = words.groupBy("value").count()
    wordcount.printSchema()
    println(wordcount.isStreaming)
    wordcount.writeStream.start("/data")

  }
  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    StructuredStreaming.worcount(sparkSession)
  }
}
