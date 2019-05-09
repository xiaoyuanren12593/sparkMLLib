package bzn.job.etl

import java.text.SimpleDateFormat
import java.util.Date

/**
  * author:xiaoYuanRen
  * Date:2019/5/8
  * Time:19:16
  * describe:Test
  **/
object test {
  def main(args: Array[String]): Unit = {

    val sr = 213.0.toLong
    println(sr)
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
}
