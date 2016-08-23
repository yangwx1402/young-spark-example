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
    val lines = Source.fromFile(new File(filePath)).getLines().toList
    val left = split(lines, { line => line.split("\t")(1).toInt  >= 400 })
    println("left size ="+left.size)
    val right = split(lines, { line => line.split("\t")(1).toInt < 400 })
    println("right size ="+right.size)
    writeLines(leftFilePath,left)
    writeLines(rightFilePath,right)
  }

  private def split(lines: List[String], function: String => Boolean): List[String] = lines.filter(function)

  private def writeLines(path:String,lines:List[String]): Unit ={
    val writer = new PrintWriter(new FileOutputStream(path))
    for(line<-lines){
      writer.write(line+"\n")
    }
    writer.flush()
    writer.close()
  }

  def getData(dataPath:String):RDD[(String,String)]={
    sparkContext.textFile(dataPath).map(line=>{
      val temp = line.split("\t")
      (temp(0),temp(1)+","+temp(2)+","+temp(3))
    })
  }

  

  def joinData(leftData:RDD[(String,String)],rightData:Map[String,String]):RDD[(String,String)]={
       leftData.map(line=>{
         if(rightData.contains(line._1)){
           (line._1,line._2+":"+rightData.get(line._1).get)
         }else{
           (line._1,line._2+":null")
         }
       })
  }

  def main(args: Array[String]) {
    val dataPath = BoradCastExample.getClass.getResource("/").getPath+File.separator+"data"+File.separator+"grouplens"+File.separator+"10k"+File.separator+"u1.base"
    BoradCastExample.splitData(dataPath)
    val leftData = getData(leftFilePath)
    val rightData = Source.fromFile(rightFilePath,"utf-8").getLines().map(f=>{
      val temp = f.split("\t")
      (temp(0),temp(1)+","+temp(2)+","+temp(3))
    }).toMap
    val result = joinData(leftData,rightData)
    result.foreach(println _)
  }
}
