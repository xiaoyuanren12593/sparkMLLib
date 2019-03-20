package xing.test

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import scala.collection.mutable.ArrayBuffer

object TestContinueMonth {
  def main(args: Array[String]): Unit = {
    val dateStr = "201802-201803-201804-201805-201901-201902-201903-201904-201905-201906"
    val res = dateStr.split("-").sorted
    val first_data = res(0)
    val final_data = res(res.length - 1)
    //2个日期相隔多少个月，包括开始日期和结束日期
    val get_res_day: ArrayBuffer[String] = getBeg_End_one_two_month(first_data, final_data)
    println("get_res_day = "+get_res_day)
    val test: String = month_add_jian(0, res(0))
    println("test = "+test)
    //找出最近的一次的连续日期
    val filter_date: String =
      if (res.length == get_res_day.length)
        month_add_jian(0, res(0))
      else {
        val res_end = get_res_day.filter(!res.contains(_)).reverse(0)
        //过滤出不存在的日期
        val resss = get_res_day.filter(!res.contains(_))
        val res_end_test = month_add_jian(1, res_end)
        println("resss = "+resss)
        month_add_jian(1, res_end)
      }
    println("filter_date = "+filter_date)
    //得到2个日期之间相隔多少个月
    val end_final: Int = getBeg_End_one_two_month(filter_date, final_data).length

    println("end_final = "+end_final)
  }

  //月份的增加和减少
  def month_add_jian(number: Int, filter_date: String): String = {
    //当前月份+1
    val sdf = new SimpleDateFormat("yyyyMM")
    val dt = sdf.parse(filter_date)
    val rightNow = Calendar.getInstance()
    rightNow.setTime(dt)
    rightNow.add(Calendar.MONTH, number)
    val dt1 = rightNow.getTime()
    val reStr = sdf.format(dt1)
    reStr
  }

  //得到2个日期之间的所有月份
  def getBeg_End_one_two_month(mon3: String, day_time: String): ArrayBuffer[String] = {
    val sdf = new SimpleDateFormat("yyyyMM")

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
      cd.add(Calendar.MONTH, 1); //增加一天 放入集合
      date = cd.getTime
    }
    arr
  }
}
