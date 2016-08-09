package com.young.scala.spark.example.streaming

import java.util

import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties
import org.apache.kafka.common.serialization.Deserializer

/**
 * Created by Administrator on 2016/8/9.
 * 实现了自己的Deserializer和Decoder,Decoder是0.8以前的版本序列化和反序列化的接口,而Deserializer是0.9以后的新接口,
 * 目前spark streaming的kafka采用的是kafka0.8的版本,所以需要实现Decoder
 */
class IntDeserializer extends Deserializer[Int]{
  override def configure(map: util.Map[String, _], b: Boolean): Unit = ???

  override def close(): Unit = ???

  def toInt(bytes:Array[Byte], offset:Int, length:Int):Int = {
    var n = 0
    for(i <- offset until (offset+length)) {
      n <<= 8
      n ^= bytes(i) & 0xFF
    }
    n

  }

  override def deserialize(s: String, bytes: Array[Byte]): Int = toInt(bytes,0,bytes.length)
}

class IntDecoder(props: VerifiableProperties = null) extends Decoder[Int]{

  val SIZEOF_INT = Integer.SIZE

  /**
   * 参考了HBase Bytes.toInt
   * @param bytes
   * @param offset
   * @param length
   * @return
   */
  def toInt(bytes:Array[Byte], offset:Int, length:Int):Int = {
      var n = 0
      for(i <- offset until (offset+length)) {
        n <<= 8
        n ^= bytes(i) & 0xFF
      }
    n

  }

  override def fromBytes(bytes: Array[Byte]): Int = toInt(bytes,0,bytes.length)
}

