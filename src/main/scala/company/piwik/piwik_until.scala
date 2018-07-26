package company.piwik

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.regex.Pattern

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
  * Created by MK on 2018/7/11.
  */
trait piwik_until {
  //kon
  def to_null(s: String): String
  = {
    if (s == null) "null" else s.toString
  }

  //将日期+8小时(24小时制)
  def eight_date(date_time: String): String
  = {
    //    val date_time = "2017-06-06 03:39:09.0"
    val sim = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = sim.parse(date_time)
    val c = Calendar.getInstance
    c.setTime(date)
    c.add(Calendar.HOUR_OF_DAY, +8)
    val newDate = c.getTime
    sim.format(newDate)
  }
  //将日期+8小时(24小时制)只有时间
  def eight_date_only_hour(date_time: String): String
  = {
    val sim = new SimpleDateFormat("HH:mm:ss")
    val date = sim.parse(date_time)
    val c = Calendar.getInstance
    c.setTime(date)
    c.add(Calendar.HOUR_OF_DAY, +8)
    val newDate = c.getTime
    sim.format(newDate)
  }

  //是否是数字
  def number_if_not(str: String): Boolean
  = {
    val pattern = Pattern.compile("^[-\\+]?[\\d]*$")
    pattern.matcher(str).matches

  }

  //将时间转换为时间戳
  def currentTimeL(str: String): Long
  = {
    val format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

    val insured_create_time_curr_one: DateTime = DateTime.parse(str, format)
    val insured_create_time_curr_Long: Long = insured_create_time_curr_one.getMillis

    insured_create_time_curr_Long
  }
}
