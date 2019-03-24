package com.atgugu.sparkmall.realtime

import java.util

import com.atgugu.sparkmall.realtime.app.AdsInfo
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object BlackListApp {
  // redis的一些相关参数
  val redisIp = "hadoop102"
  val redisPort = 6379
  val countKey = "user:day:adsclick"
  val blackListKey = "blacklist"

  //过滤黑名单中的用户，返回值式不包含黑名单的用户的广告点击记录的DStream
  def checkUserFromBlackList(adsClickInfoDStream:DStream[AdsInfo],sc:SparkContext) ={
   adsClickInfoDStream.transform{
     rdd=>{
       val jedis = new Jedis(redisIp,redisPort)
       val blackList: util.Set[String] = jedis.smembers(blackListKey)
       val BlackListBC: Broadcast[util.Set[String]] = sc.broadcast(blackList)
       jedis.close()
       rdd.filter(adsinfo =>
         if(blackList==null){
           !blackList.contains(adsinfo.userid)
         }else{
           true
         }
       )
       
     }
   }
  }

  def checkUserToBlackList(adsInfoDStream: DStream[AdsInfo]) = {
    adsInfoDStream.foreachRDD(rdd=>{
       rdd.foreachPartition(adsinfoIt=>{
         val jedis = new Jedis(redisIp,redisPort)
          adsinfoIt.foreach{adsinfo=>
            val countField : String = s"${adsinfo.userid}:${adsinfo.dayString}:${adsinfo.adid}"
            jedis.hincrBy(countKey,countField,1)
            if(jedis.hget(countKey,countField).toLong >= 100){
              jedis.sadd(blackListKey, adsinfo.userid)
            }
          }
         jedis.close()
       })
      }
    )
  }



}
