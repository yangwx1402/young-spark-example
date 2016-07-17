package com.young.scala.spark.example.mllib.dataype

import com.young.scala.spark.example.transformation.BaseExample
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{ListBuffer, ArrayBuffer}
import scala.util.Random

/**
 * Created by Administrator on 2016/7/17.
 */
class DistributedMatrixs extends BaseExample{

  val random = new Random

  /**
   * RowMatrix分步式矩陣,該矩陣沒有索引信息,也就是說沒有标签
   * @param rowNum
   * @param colNum
   * @return
   */
  def getRowMatrix(rowNum:Int,colNum:Int):RowMatrix={
    val seq = Array.fill(rowNum)({
      Vectors.dense(Array.fill(colNum)(random.nextDouble()))
    }).toSeq
    val vectorRdd : RDD[Vector] = sparkContext.parallelize(seq)
    new RowMatrix(vectorRdd)
  }

  /**
   * 带有行标号的分布式矩阵,该矩阵每行都含有标签信息,IndexedRowMatrix可以转换成RowMatrix,也就是去掉标号即可
   * @param rowNum
   * @param colNum
   * @return
   */
  def getIndexedRowMatrix(rowNum:Int,colNum:Int):IndexedRowMatrix={
    var num:Long = 0
    val seq = Array.fill(rowNum)({
      num+=1
      new IndexedRow(num,Vectors.dense(Array.fill(colNum)(random.nextDouble())))
    }).toSeq
    val vectorRdd :RDD[IndexedRow] = sparkContext.parallelize(seq)
    new IndexedRowMatrix(vectorRdd)
  }

  /**
   *CoordinateMatrix采用(row,col,value)的方式进行存储,主要用于巨大稀疏矩阵的分布式存储
   * @return
   */
  def getCoordinateMatrix(rowNum:Long,colNum:Long):CoordinateMatrix={
    val arrayBuffer = new ListBuffer[MatrixEntry]()
    for(i<-0l until rowNum){
      for(j<-0l until colNum){
        arrayBuffer.append(new MatrixEntry(i,j,random.nextDouble()))
      }
    }
    val seq = arrayBuffer.toSeq
    val vectorRdd : RDD[MatrixEntry] = sparkContext.parallelize(seq)
    new CoordinateMatrix(vectorRdd)
  }

}
object DistributedMatrixs{
  def main(args: Array[String]) {
    val test = new DistributedMatrixs
    test.getRowMatrix(10,10).rows.foreach(println _)
    test.getIndexedRowMatrix(10,10).rows.foreach(println _)
    test.getCoordinateMatrix(10,10).entries.foreach(println _)
  }
}

