package com.bzn.cLabel

import java.text.{NumberFormat, SimpleDateFormat}
import java.util.{Calendar, Date}

import scala.collection.mutable.ArrayBuffer

object test {
  def main(args: Array[String]): Unit = {
    println(getDay.split(" ")(0).replace("-", ""))
    val str = "2017-10-09 08:07:34.0"
//    val insured_start_date = "2017-11-12 07:24:06".toString.split(" ")(0).replaceAll("-", "").replaceAll("/", "")
//    val insured_end_date = "2017-11-12 23:59:59".toString.split(" ")(0).replaceAll("-", "").replaceAll("/", "")
//    println(getBeg_End_one_two(insured_start_date, insured_end_date).size)
//    val test =""+"\u0001"+"123"
//    println(eight_date_only_hour("04/12/19"))
    println(str.substring(0,str.length-2))
//    val rs = "0".toInt
//    println(1 / 365.toDouble*246)
//    val re = "12.2633".toDouble
//    var numberFormat = NumberFormat.getInstance
//    // 设置精确到小数点后2位
//    numberFormat.setMaximumFractionDigits(1)
//    println(numberFormat.format(re))

//    println(re)
//    91110108348452091Y
//    231085199502280515
//    val shen = "231085199502280515"
//    println(shen.substring(shen.length-2,shen.length-1))
//    println(cardCodeVerifySimple(shen))
//    println(test.trim.split("\u0001").distinct.length)
//    println(test.trim.split("\u0001").distinct.mkString("\u0001").split("\u0001").length)
//    println(str.length)
//    if(str == ""){
//      println("this is null")
//    }
  }

  //将日期+8小时(24小时制)只有时间
  def eight_date_only_hour(date_time: String): String = {
    val sim = new SimpleDateFormat("dd/MM/yy")
    val date = sim.parse(date_time)
    val daFormat = new SimpleDateFormat("yyyy-MM-dd")
    daFormat.format(date)
  }

  private def cardCodeVerifySimple(cardcode: String): Boolean = {
    val isIDCard1 = "^[1-9]\\d{7}((0\\d)|(1[0-2]))(([0|1|2]\\d)|3[0-1])\\d{3}$"
    //第二代身份证正则表达式(18位)
    val isIDCard2 = "^[1-9]\\d{5}[1-9]\\d{3}((0\\d)|(1[0-2]))(([0|1|2]\\d)|3[0-1])((\\d{4})|\\d{3}[A-Z])$"
    //验证身份证
    if (cardcode.matches(isIDCard1) || cardcode.matches(isIDCard2))
      return true
    false
  }


  //得到2个日期之间的所有天数
  def getBeg_End_one_two(mon3: String, day_time: String): ArrayBuffer[String]
  = {
    val sdf = new SimpleDateFormat("yyyyMMdd")

    //得到过去第三个月的日期
    val c = Calendar.getInstance
    c.setTime(new Date)
    c.add(Calendar.MONTH, -3)
    val m3 = c.getTime

    //得到今天的日期
    val cc = Calendar.getInstance
    cc.setTime(new Date)
    val day = cc.getTime

    //得到他们相间的所有日期
    val arr: ArrayBuffer[String] = ArrayBuffer[String]()
    val date_start = sdf.parse(mon3)
    //    val date_start = sdf.parse("20161007")
    val date_end = sdf.parse(day_time)
    //    val date_end = sdf.parse("20161008")
    var date = date_start
    val cd = Calendar.getInstance //用Calendar 进行日期比较判断

    while (date.getTime <= date_end.getTime) {
      arr += sdf.format(date)
      cd.setTime(date)
      cd.add(Calendar.DATE, 1); //增加一天 放入集合
      date = cd.getTime
    }
    arr

  }

  def getDay: String
  = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    //得到前7天的日期是几号
    val c = Calendar.getInstance()
    val res = c.getTime
    sdf.format(c.getTime)
  }
}
