package com.young.scala.spark.example.broadcast

import java.io.{File, FileOutputStream, PrintWriter}

import com.young.scala.spark.example.transformation.BaseExample
import org.apache.spark.rdd.RDD

import scala.io.Source

/**
 * Created by dell on 2016/8/19.
 */
object BoradCastExample extends BaseExample {

  val leftFilePath = BoradCastExample.getClass.getResource("/").getPath+File.separator+"data"+File.separator+"join"+File.separator+"left.data"
  val rightFilePath = BoradCastExample.getClass.getResource("/").getPath+File.separator+"data"+File.separator+"join"+File.separator+"right.data"

  private def splitData(filePath: String): Unit = {
    val lines = Source.fromFile(new File(filePath)).getLines()
    val left = split(lines, { line => line.split("\t")(0).toInt % 2 == 0 })
    val right = split(lines, { line => line.split("\t")(0).toInt % 2 == 1 })
    writeLines(leftFilePath,left)
    writeLines(rightFilePath,right)
  }

  private def split(lines: Iterator[String], function: String => Boolean): List[String] = lines.filter(function).toList

  private def writeLines(path:String,lines:List[String]): Unit ={
    val writer = new PrintWriter(new FileOutputStream(path))
    for(line<-lines){
      writer.write(line)
    }
    writer.flush()
    writer.close()
  }

  def getData(dataPath:String):RDD[(String,String)]={
    sparkContext.textFile(dataPath).map(line=>{
      val temp = line.split("/t")
      (temp(0),temp(1)+","+temp(2)+","+temp(3))
    })
  }

  

  def joinData(leftData:RDD[String],rightData:List[String]):Unit={

  }

  def main(args: Array[String]) {
    val dataPath = BoradCastExample.getClass.getResource("/").getPath+File.separator+"data"+File.separator+"grouplens"+File.separator+"10k"+File.separator+"u1.base"
    BoradCastExample.splitData(dataPath)
    val leftData = getData(leftFilePath)
    val rightData = Source.fromFile(rightFilePath,"utf-8").getLines().toM
    joinData(leftData)
  }
}
