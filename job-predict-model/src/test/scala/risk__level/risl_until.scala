package risk__level

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable.ArrayBuffer

/**
  * Created by MK on 2018/5/17.
  */
trait risl_until {

  //得到2个日期之间的所有天数
  def getBeg_End_one_two(mon3:String,day_time:String): ArrayBuffer[String] = {
    val sdf = new SimpleDateFormat("yyyy/MM/dd")

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

  //将时间转换为时间戳
  def currentTimeL(str: String): Long
  = {
    val format = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss")

    val insured_create_time_curr_one: DateTime = DateTime.parse(str, format)
    val insured_create_time_curr_Long: Long = insured_create_time_curr_one.getMillis

    insured_create_time_curr_Long
  }
}
