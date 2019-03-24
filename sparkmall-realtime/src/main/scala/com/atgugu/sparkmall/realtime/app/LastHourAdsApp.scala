package com.atgugu.sparkmall.realtime.app

import java.text.SimpleDateFormat

import com.atguigu.sparkmall.common.util.RedisUtil
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object LastHourAdsApp {
  def statLastHourAdsClick(filteredDStream: DStream[AdsInfo]) = {
    val windowDStream: DStream[AdsInfo] = filteredDStream.window(Minutes(60),Seconds(4))
    val groupAdsCountDStream: DStream[(String, Iterable[(String, Int)])] = windowDStream.map(adsInfo => {
      val houreMinutes = new SimpleDateFormat("HH:mm").format(adsInfo.timestamp)
      ((adsInfo.adid, houreMinutes), 1)
    }).reduceByKey(_ + _)
      .map {
        case ((adsId, hourMinutes), count) => {
          (adsId, (hourMinutes, count))
        }
      }.groupByKey
    val jsonCountDStream: DStream[(String, String)] = groupAdsCountDStream.map {
      case (adsId, it) => {
        import org.json4s.JsonDSL._
        val hourMinutesJson: String = JsonMethods.compact(JsonMethods.render(it))
        (adsId, hourMinutesJson)
      }
    }
    jsonCountDStream.foreachRDD(rdd=>{
      val result: Array[(String, String)] = rdd.collect
      import collection.JavaConversions._
      val client: Jedis = RedisUtil.getJedisClient
      client.hmset("last_hour_ads_click",result.toMap)
      client.close()
    })
  }
}
