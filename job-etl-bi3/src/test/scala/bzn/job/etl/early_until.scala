package bzn.job.etl

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, UUID}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by MK on 2018/7/17.
  */
trait early_until {
  //得到2个日期之间的所有日期(当天和3个月之前的)
  def getBeg_End: ArrayBuffer[String] = {
    val sdf = new SimpleDateFormat("yyyy/M/d")

    //得到前7天的日期是几号
    val c = Calendar.getInstance()
    c.add(Calendar.DAY_OF_MONTH, -1500)
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

  //得到当前日期
  def getDay: String
  = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    //得到前7天的日期是几号
    val c = Calendar.getInstance()
    sdf.format(c.getTime)
  }

  //生成唯一的ID
  def getUUID: String = {
    UUID.randomUUID.toString.replace("-", "")

  }
  //是否是null
  def is_noy_null(res: Any): Double
  = {
    if (res == null) 0.0 else res.toString.toDouble
  }
}
