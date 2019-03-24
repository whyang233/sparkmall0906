package com.atguigu.sparkmall.offline.bean.com.atguigu.sparkmall.offline.ACC

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class acc extends AccumulatorV2[(String,String),mutable.Map[(String,String),Long]]{
  private val map = new mutable.HashMap[(String,String),Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[(String, String), mutable.Map[(String, String), Long]] = {
    val acc = new acc
    map.synchronized{
      acc.map ++= this.map
    }
    acc
  }

  override def reset(): Unit = map.clear()

  override def add(v: (String, String)): Unit = {
    map(v) = map.getOrElse(v,0L) + 1L
  }

  override def merge(other: AccumulatorV2[(String, String), mutable.Map[(String, String), Long]]): Unit = {
    other.value match {
      case value=>{
        value.foreach(x =>this.map.put(x._1 , this.map.getOrElse(x._1,0L) + x._2 ) )
      }
    }
  }

  override def value: mutable.Map[(String, String), Long] = map
}
