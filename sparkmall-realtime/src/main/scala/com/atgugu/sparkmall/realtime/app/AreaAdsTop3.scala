package com.atgugu.sparkmall.realtime.app

import com.atguigu.sparkmall.common.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object AreaAdsTop3 {
  def statAreaAdsTop3(dayAreaAdsCount:DStream[(String,Int)])={
    // 1.去掉城市
    val groupByDayDStream: DStream[(String, Iterable[(String, (String, Int))])] = dayAreaAdsCount.map {
      case (dayAreaAdscity, count) => {
        //2019-03-11:华南：深圳：1，2
        val split: Array[String] = dayAreaAdscity.split(",")
        (s"${split(0)}:${split(1)}:${split(3)}", count)
      }
    }.reduceByKey(_ + _)
      .map {
        case (dayAreaAds, count) => {
          val splits: Array[String] = dayAreaAds.split(":")
          (splits(0), (splits(1), (splits(2), count)))
        }
      }
      .groupByKey
    val resultDstream: DStream[(String, Map[String, String])] = groupByDayDStream.map {
      case (day, it: Iterable[(String, (String, Int))]) => {
        val temp1: Map[String, Iterable[(String, (String, Int))]] = it.groupBy(_._1)
        val temp2: Map[String, Iterable[(String, Int)]] = temp1.map {
          case (day, it) => {
            (day, it.map(_._2))
          }
        }
        val temp3 = temp2.map {
          case (day, it) => {
            val list: List[(String, Int)] = it.toList.sortWith(_._2 > _._2).take(3)
            import org.json4s.JsonDSL._
            //compact压平字符串
            val adsCountJsonString: String = JsonMethods.compact(JsonMethods.render(list))
            (day, adsCountJsonString)
          }
        }
        (day, temp3)
      }
    }
    resultDstream.foreachRDD(rdd=>{
      val dayAreaAdsCountArray: Array[(String, Map[String, String])] = rdd.collect

      val client: Jedis = RedisUtil.getJedisClient
      dayAreaAdsCountArray.foreach({
        case (day,map)=>{
          //用来把scala的map转成java的map
          import scala.collection.JavaConversions._
          client.hmset("area:ads:top3:" + day,map)
        }
      })
    })
    
  }
}
