package com.atguigu.sparkmall.offline.app

import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.common.util.JDBCUtil
import com.atguigu.sparkmall.offline.bean.com.atguigu.sparkmall.offline.ACC.acc
import com.atguigu.sparkmall.offline.bean.com.atguigu.sparkmall.offline.util.CategoryCountInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object CategoryTop10App {

  def statCategoryTop10(spark:SparkSession,userVisitActionRDD:RDD[UserVisitAction],taskID :String) ={
    //注册累加器
    val acc = new acc
    spark.sparkContext.register(acc,"CategoryActionAcc")

    //遍历日志
    userVisitActionRDD.foreach{
      visitAction =>{
        if(visitAction.click_category_id != -1){
          acc.add((visitAction.click_category_id.toString,"click"))
        }else if(visitAction.order_category_ids != null){
          visitAction.order_category_ids.split(",").foreach{
            odi => acc.add((odi,"order"))
          }
        }else if(visitAction.pay_category_ids != null){
          visitAction.pay_category_ids.split(",").foreach{
            pid => acc.add((pid,"pay"))
          }
        }
      }
    }

    //3.遍历完成后就得到每个品类的id和操作类型的数量，然后按照categoryId 进行分组
    val actionCountByCategoryIdMap: Map[String, mutable.Map[(String, String), Long]] = acc.value.groupBy(_._1._1)

    val categoryCountInfoList: List[CategoryCountInfo] = actionCountByCategoryIdMap.map {
      case (cid, actionMap) => CategoryCountInfo(
        taskID,
        cid,
        actionMap.getOrElse((cid, "click"), 0),
        actionMap.getOrElse((cid, "order"), 0),
        actionMap.getOrElse((cid, "pay"), 0)
      )
    }.toList

    //5.按照 点击 下单 支付 的顺序降序来排序
    val infoes: List[CategoryCountInfo] = categoryCountInfoList.sortBy(info => (info.clickCount,info.orderCount,info.payCount))(Ordering.Tuple3(Ordering.Long.reverse,Ordering.Long.reverse,Ordering.Long.reverse))

    //6.截取前10
    val top10: List[CategoryCountInfo] = infoes.take(10)

    top10.foreach(println)

    //写入Mysql  使用批处理器
    //6.1 表中的数据情况
    JDBCUtil.executeUpdate("use sparkmall",null)
    JDBCUtil.executeUpdate("truncate table category_top10",null)
    //6.2 真正的插入数据
    //转换数据结构
    val top10Array: List[Array[Any]] = top10.map(info => Array(info.taskId,info.categoryId,info.clickCount,info.orderCount,info.payCount))
    JDBCUtil.executeBatchUpdate("insert into category_top10 values(?, ?, ?, ?, ?)", top10Array)

    top10
  }
}
