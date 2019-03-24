package com.atguigu.sparkmall.offline.bean.com.atguigu.sparkmall.offline.udf

import java.text.DecimalFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


class CityClickCountUDAF extends UserDefinedAggregateFunction{

  /*
  城市名 ->   城市点击数占有率
  1.输入类型：城市名 -> String
  2.中间缓冲类型:
  如： 城市名：数量
  map[String,Ling]
  3.输入类型
  String
   */


  //输入类型
  override def inputSchema: StructType = {
    StructType(StructField("city_name",StringType)::Nil)
  }

  //中间缓存数据类型
  override def bufferSchema: StructType = {
    StructType(StructField("city_count",MapType(StringType,LongType))::StructField("total_count",LongType)::Nil)
  }

  //输入类型
  override def dataType: DataType = StringType

  //是否输出和输入的类型相同
  override def deterministic: Boolean = true

  //输入类型初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String,Long]()
    buffer(1) = 0L
  }

  //分区内数据聚合
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val cityName: String = input.getString(0)
    val map: Map[String, Long] = buffer.getAs[Map[String,Long]](0)
    buffer(0) = map + (cityName -> (map.getOrElse(cityName,0L) + 1))

    //总数加1
    buffer(1) = buffer.getLong(1) + 1L
  }

  //分区间数据聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //map聚合
    val map1: Map[String, Long] = buffer1.getAs[Map[String, Long]](0)
    val map2: Map[String, Long] = buffer2.getAs[Map[String, Long]](0)
    buffer1(0) = map2.foldLeft(map1){
      case (m,(k,v)) => m + ((k , (m.getOrElse(k,0L) + v)))
    }

    //总数聚合
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //中间缓存转换问输出数据类型
  override def evaluate(buffer: Row): Any = {
    val map: Map[String, Long] = buffer.getAs[Map[String,Long]](0)
    val total_count: Long = buffer.getLong(1)

    //求出占有率
    val cityTop2: List[(String, Long)] = map.toList.sortBy(_._2)(Ordering.Long.reverse)take(2)
    val remarks: List[CityRemark] = cityTop2.map {
      case (city, count) => CityRemark(city, count.toDouble / total_count)
    }
    println(remarks.mkString(","))
    val allRate: List[CityRemark] = remarks :+ CityRemark("其他",remarks.foldLeft(1D)((total,remark)=>total - remark.Rate))

    allRate.mkString(",")
  }
}

case class CityRemark(cityName : String , Rate : Double){
  override def toString: String = s"${cityName}${format.format(Rate)}"
  val format = new DecimalFormat("0.0%")
}
