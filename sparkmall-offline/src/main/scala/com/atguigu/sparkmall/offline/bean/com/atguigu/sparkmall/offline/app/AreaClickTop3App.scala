package com.atguigu.sparkmall.offline.bean.com.atguigu.sparkmall.offline.app

import java.util.Properties

import com.atguigu.sparkmall.common.util.ConfigurationUtil
import com.atguigu.sparkmall.offline.bean.com.atguigu.sparkmall.offline.udf.CityClickCountUDAF
import org.apache.spark.sql.{SaveMode, SparkSession}


object AreaClickTop3App {
  def statAreaClickTop3Product(spark:SparkSession,taskId:String): Unit ={
    spark.udf.register("city_rate",new CityClickCountUDAF)
    spark.sql(
      """
        |select
        |c.*,
        |click_product_id
        |from city_info c join user_visit_action v
        |on c.city_id = v.city_id
        |where v.click_product_id > -1
      """.stripMargin).createOrReplaceTempView("t1")

    spark.sql(
      """
        |select
        |area,
        |product_name,
        |count(*) click_count,
        |city_rate(t1.city_name)
        |from product_info p join t1
        |on p.product_id = t1.click_product_id
        |group by area,product_name
      """.stripMargin).createOrReplaceTempView("t2")

    spark.sql(
      """
        |select
        |*,
        |rank() over(partition by area order by click_count ) rank
        |from t2
      """.stripMargin).createOrReplaceTempView("t3")

    val conf = ConfigurationUtil("config.properties")
    val props = new Properties()
    props.setProperty("user", conf.getString("jdbc.user"))
    props.setProperty("password", conf.getString("jdbc.password"))
    spark.sql(
      """
        |select
        |*
        |from t3
        |where rank <=3
      """.stripMargin).write
      .mode(SaveMode.Overwrite)
      .jdbc(conf.getString("jdbc.url"),"area_click_top10",props)
  }

}
