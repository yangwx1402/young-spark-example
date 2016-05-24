package com.young.scala.spark.example.transformation

import java.io.File

/**
 * Created by Administrator on 2016/5/24.
 */
object MapExample extends App with BaseExample{

  def map(): Unit ={
    val textFile = MapExample.getClass.getResource("/").getPath+File.separator+"data"+File.separator+"map.data"
    val data = sparkContext.textFile(textFile).flatMap(_.split(" ")).map((_,1)).collect()
    data.foreach(println _)
  }
}
