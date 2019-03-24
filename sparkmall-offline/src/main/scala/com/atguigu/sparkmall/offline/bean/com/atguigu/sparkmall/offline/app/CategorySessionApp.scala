package com.atguigu.sparkmall.offline.bean.com.atguigu.sparkmall.offline.app

import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.common.util.JDBCUtil
import com.atguigu.sparkmall.offline.bean.com.atguigu.sparkmall.offline.bean.CategorySession
import com.atguigu.sparkmall.offline.bean.com.atguigu.sparkmall.offline.util.CategoryCountInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CategorySessionApp {
  def statCategoryTop10Session(spark:SparkSession,categoryCountInfo: List[CategoryCountInfo],userVisitActionRDD: RDD[UserVisitAction],taskId:String): Unit ={
    //1.Category Top10 只需要拿到CategoryID
    val top10CategoryId: List[String] = categoryCountInfo.map(_.categoryId)

    //2.从RDD[UserVisitAction].filter(cids.contains(_.CategoryId))
    val filteredActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(
      userAction => top10CategoryId.contains(userAction.click_category_id.toString)
    )

    //3.类型的转换CategoryCountInfo(201341d2-d6fb-4f70-b2e4-3d0debf560ae,15,194,36,37)
    //=>((cid,sid),1)
    val cid_sid_count_RDD: RDD[(Long, (String, Int))] = filteredActionRDD.map {
      userAction => ((userAction.click_category_id, userAction.session_id), 1)
    }.reduceByKey(_ + _)
      .map {
        case ((cid, sid), count) => (cid, (sid, count))
      }

    //按key进行分组
    val groupCategoryCountRDD: RDD[(Long, Iterable[(String, Int)])] = cid_sid_count_RDD.groupByKey()
    val SeesionCount: RDD[CategorySession] = groupCategoryCountRDD.flatMap {
      case (cid, item) => {
        item.toList.sortBy(_._2)(Ordering.Int.reverse).take(10).map {
          case (sid, count) => CategorySession(taskId, cid.toString, sid, count)
        }
      }
    }

    SeesionCount.collect().foreach(println)

    //5.写入MySQL
    val SeesionCountArray: Array[Array[Any]] = SeesionCount.collect().map(item=>Array(item.taskId,item.categoryId,item.sessionId,item.clickCount))
    JDBCUtil.executeUpdate("truncate category_top10_session_count",null)
    JDBCUtil.executeBatchUpdate("insert into category_top10_session_count value(?,?,?,?)",SeesionCountArray)
  }
}
