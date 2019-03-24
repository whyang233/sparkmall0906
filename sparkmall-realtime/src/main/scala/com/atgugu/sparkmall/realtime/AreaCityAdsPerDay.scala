package com.atgugu.sparkmall.realtime

import com.atgugu.sparkmall.realtime.app.AdsInfo
import com.atguigu.sparkmall.common.util.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object AreaCityAdsPerDay {
  def statAreaCityAdsPerDay(filteredAdsInfoDStream: DStream[AdsInfo], sc: SparkContext) = {
    sc.setCheckpointDir("hdfs://hadoop102:9000/checkpoint")
    val resultDS: DStream[(String, Long)] = filteredAdsInfoDStream.map(adsinfo =>
      (s"${adsinfo.dayString}:${adsinfo.area}:${adsinfo.city}:${adsinfo.adid}", 1L)
    ).reduceByKey(_ + _)
      .updateStateByKey {
        case (seq: Seq[Long], opt: Option[Int]) => {
          Some(seq.sum + opt.getOrElse(0L))
        }
      }

    resultDS.foreachRDD(rdd=>{
      val jedisClient: Jedis = RedisUtil.getJedisClient

      val resultArr: Array[(String, Long)] = rdd.collect
      resultArr.foreach{
        case (field,count) =>{jedisClient.hset("day:area:city:adsCount",field,count.toString)}
      }
      jedisClient.close()
    })

    resultDS
  }
}
