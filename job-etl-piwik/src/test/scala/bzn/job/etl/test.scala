package bzn.job.etl

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  * author:xiaoYuanRen
  * Date:2019/5/8
  * Time:19:16
  * describe:Test
  **/
object test {
  def main(args: Array[String]): Unit = {

    val sr = 213.0
    println(dateAddOneDay("2017-12-31 12:00:00"))
//    val res = get_current_date(str)
//    println(res)
  }
  //时间戳转换为日期
  def get_current_date(current: Long): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //这个是你要转成后的时间的格式, 时间戳转换成时间
    val sd = sdf.format(new Date(current));
    sd
  }

  //当前日期+1天
  def dateAddOneDay(date_time: String): String = {
    //    val date_time = "2017-06-06 03:39:09.0"
    val sim = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = sim.parse(date_time)
    val c = Calendar.getInstance
    c.setTime(date)
    c.add(Calendar.DATE, 1)
    val newDate = c.getTime
    sim.format(newDate)
  }
}
