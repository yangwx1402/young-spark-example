package com.young.scala.spark.example.dataframe

import com.young.scala.spark.example.transformation.BaseExample
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by Administrator on 2016/7/15.
 */
object DataFrameStudy extends BaseExample {
  System.setProperty("HADOOP_USER_NAME", "hadoopadmin")
  System.setProperty("HADOOP_PROXY_USER","hadoopadmin")

  sparkContext.addJar("E:\\project\\young\\scala\\young-spark-example\\target\\scala-2.10\\young-spark-example_2.10-1.0.jar")

  println(sparkContext)
  val hiveContext = new HiveContext(sparkContext)

  def readTableData(tableName: String): DataFrame = {
    val sql = "select * from " + tableName
    readSqlData(sql)
  }

  def readSqlData(sql: String): DataFrame = {
    hiveContext.sql(sql)
  }

  def main(args: Array[String]) {
    val data = DataFrameStudy.readTableData("test")
    data.foreach(println _)
  }

}
