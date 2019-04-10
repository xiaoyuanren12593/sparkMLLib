package bzn.job.etl

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import scala.collection.mutable.ArrayBuffer

object test {
  def main(args: Array[String]): Unit = {
    val ss = java.math.BigDecimal.valueOf(99.4442132441200).toString.toDouble
    val ss1 = java.math.BigDecimal.valueOf(0E-18).toString.toDouble
    if(ss != ss1){
      println("相等"+ss)
    }
    println(getBeg_End)
  }

  def getBeg_End: ArrayBuffer[String] = {
    val sdf = new SimpleDateFormat("yyyy/M/d")

    //得到前7天的日期是几号
    val c = Calendar.getInstance()
    c.add(Calendar.DAY_OF_MONTH, -150)
    val mon3 = sdf.format(c.getTime)


    //得到今天的日期
    val cc = Calendar.getInstance
    cc.setTime(new Date)
    val day = cc.getTime
    val day_time = sdf.format(day)


    //得到他们相间的所有日期
    val arr: ArrayBuffer[String] = ArrayBuffer[String]()
    val date_start = sdf.parse(mon3)
    //        val date_start = sdf.parse("2016/10/8")
    val date_end = sdf.parse(day_time)
    //        val date_end = sdf.parse("2016/10/12")
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
}
