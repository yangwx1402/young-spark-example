package com.young.scala.spark.example.stocks

import java.io.{File, FileInputStream}

import org.apache.commons.io.{FileUtils, IOUtils}

/**
 * Created by dell on 2016/9/30.
 */
object StocksDataTrasferTools {
  def mergeData(dataDir: String, distFile: String, featureNum: Int,length:Int): Unit = {
    val file = new File(dataDir)
    val files = file.listFiles()
    var tempLength = 0
    for (f <- files) {
      val stockCode = f.getName.split("\\.")(0)
      val lines = IOUtils.readLines(new FileInputStream(f), "utf-8").toArray
      var count = 0
      if (lines.length >= featureNum)
        for (line <- lines) {
          if (count < featureNum)
            if (!line.toString.startsWith("date")) {
              FileUtils.write(new File(distFile), stockCode + "," + line + "\n", true)
              count += 1
            }
        }
      tempLength+=1
      if(tempLength == length)
        return
    }
  }


  def main(args: Array[String]) {
    val dataDir = "E:\\project\\young\\scala\\young-spark-example\\src\\main\\resources\\data\\stocks\\data"
    val distFile = "E:\\project\\young\\scala\\young-spark-example\\src\\main\\resources\\data\\stocks\\transfer\\last_300.data"
    StocksDataTrasferTools.mergeData(dataDir, distFile, 300,1000)
  }
}
