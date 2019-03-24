package com.atgugu.sparkmall.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

case class AdsInfo(timestamp : Long, area: String, city: String, userid: String, adid: String){
  val dayString: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(timestamp))
  override def toString: String = s"$dayString:$area:$city:$adid"
}
