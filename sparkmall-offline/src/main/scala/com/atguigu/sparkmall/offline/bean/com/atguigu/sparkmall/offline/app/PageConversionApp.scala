package com.atguigu.sparkmall.offline.bean.com.atguigu.sparkmall.offline.app

import java.text.DecimalFormat

import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.common.util.JDBCUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object PageConversionApp {
  def aclc(spark:SparkSession,userVisitActionRDD:RDD[UserVisitAction],targetPages:String,taskId: String): Unit ={
    //先拿到目标页面
    val pages: Array[String] = targetPages.split(",")
    val prePages: Array[String] = pages.slice(0,pages.length - 1)
    val postPages: Array[String] = pages.slice(1,pages.length)
    val targetJumpPages: Array[String] = prePages.zip(postPages).map {
      case (p1, p2) => p1 + "->" + p2
    }

    //2.计算每个目标页面的点击次数
    //2.1获取到包含目标页面的User Visit Action RDD
    val targetUserVisitAction: RDD[UserVisitAction] = userVisitActionRDD.filter(action => prePages.contains(action.page_id.toString))
    //2.2按照page_id计算点击次数
    val targetPagesCountMap: collection.Map[Long, Long] = targetUserVisitAction.map {
      action => (action.page_id, 1)
    }.countByKey

    //3.统计跳转流的次数
    //3.1 根据session分组
    val jumpFlow: RDD[String] = userVisitActionRDD.groupBy(_.session_id).flatMap {
      case (sid, it) => {
        val sortedList: List[UserVisitAction] = it.toList.sortBy(_.action_time)
        val preAction: List[UserVisitAction] = sortedList.slice(0, sortedList.length - 1)
        val postAction: List[UserVisitAction] = sortedList.slice(1, sortedList.length)
        preAction.zip(postAction).map {
          case (action1, action2) => action1.page_id + "->" + action2.page_id
        }
      }
    }
    val targetJumpFlow: RDD[String] = jumpFlow.filter(flow => targetJumpPages.contains(flow))

    //3.2统计跳转流的次数
    val countFlow: RDD[(String, Int)] = targetJumpFlow.map((_,1)).reduceByKey(_ + _)

    //4.计算跳转率
    val targetJumpRate: RDD[(String, String, Any)] = countFlow.map {
      case (flow, count) => {
        val rating = count.toDouble / targetPagesCountMap.getOrElse(flow.split("->")(0).toLong, 1L)
        (taskId, flow, new DecimalFormat("0.00%").format(rating))
      }
    }

    val insertArray: Array[Array[Any]] = targetJumpRate.collect().map(item=>Array(item._1,item._2,item._3))

    //写入Mysql
    JDBCUtil.executeUpdate("truncate table page_conversion_rate",null)
    JDBCUtil.executeBatchUpdate("insert into page_conversion_rate values(?,?,?)",insertArray)
  }
}
