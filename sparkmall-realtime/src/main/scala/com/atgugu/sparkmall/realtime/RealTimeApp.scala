package com.atgugu.sparkmall.realtime

import com.atgugu.sparkmall.realtime.app.{AdsInfo, AreaAdsTop3}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.atguigu.sparkmall.common.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

object RealTimeApp {
  def main(args: Array[String]): Unit = {
    //创建sparkconf()
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("RealTimeApp")

    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc,Seconds(3))

    //得到Dstream流
    val recordDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getDStream(ssc,"ads_log")

    //把消费的到的字符串封装到对象中
    val AdDS: DStream[AdsInfo] = recordDStream.map {
      record =>
        val splits: Array[String] = record.value.split(",")
        AdsInfo(splits(0).toLong, splits(1), splits(2), splits(3), splits(4))
    }

    //过滤黑名单成员
    val NotInBlackList: DStream[AdsInfo] = BlackListApp.checkUserFromBlackList(AdDS,sc)
    //检测是否加入黑名单
    BlackListApp.checkUserToBlackList(NotInBlackList)

    AreaAdsTop3.statAreaAdsTop3()
    ssc.start()
    ssc.awaitTermination()
  }
}
