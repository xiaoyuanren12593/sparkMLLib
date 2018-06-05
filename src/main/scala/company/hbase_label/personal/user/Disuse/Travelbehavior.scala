package company.hbase_label.personal.user.Disuse

import java.text.NumberFormat
import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}

import com.alibaba.fastjson.JSONObject
import company.hbase_label.until
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by a2589 on 2018/4/3.
  */

object Travelbehavior extends until {


  //列族:goout
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf_s = new SparkConf().setAppName("wuYu")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
      .set("spark.sql.broadcastTimeout", "36000")
    //      .setMaster("local[2]")
    val sc = new SparkContext(conf_s)
    val sqlContext: HiveContext = new HiveContext(sc)


    val open_ofo_policy_parquet: RDD[(String, String, String, Int, Int, Boolean, String)] = sqlContext
      .sql("select * from odsdb_prd.open_ofo_policy_parquet")
      //      .sql("select * from odsdb_prd.open_ofo_policy_parquet where day_id in(20180223,20180222,20161201)")
      .filter("length(insured_cert_no) =18  and length(start_date)>14")
      .map(x => {
        val formatter_before = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        val qs_hs = new DateTimeFormatterBuilder().append(formatter_before).toFormatter()


        val cert = x.getAs("insured_cert_no").toString
        val start_date = x.getAs("start_date").toString.replaceAll("/", "-")
        val de_start_date = deletePoint(start_date)
        val cert_Eighteen = cert.substring(0, cert.length - 1)
        val result_sex: Boolean = cert_Eighteen.matches("[0-9]+") //得到身份证前18位，并判断是否为数字

        //得到今天是星期几
        val week_num = getWeekOfDate(de_start_date.split(" ")(0))
        //保单号
        val product_code = x.getAs("product_code").toString
        //得到日期的小时
        val lod_before = LocalDateTime.parse(de_start_date, qs_hs)
        val hour = lod_before.getHour

        (cert, product_code, de_start_date, week_num, hour, result_sex, x.getAs("day_id").toString)
      }).filter(_._6 == true)

    val numberFormat = NumberFormat.getInstance
    numberFormat.setMaximumFractionDigits(2)

    //HBaseConf
    val conf = HbaseConf("labels:label_user_personal_vT")._1
    val conf_fs = HbaseConf("labels:label_user_personal_vT")._2
    val tableName = "labels:label_user_personal_vT"
    val columnFamily1 = "goout"
    //得到最近3个月的所有日期
    val date_three_date = getBeg_End().toArray
    val date_three_date_sc = sc.broadcast(date_three_date)
    //早起族|夜猫子|上班族|主要骑行日期|骑行频率
    val user_person_ofo_par = open_ofo_policy_parquet.mapPartitions(rdd => {
      val res = rdd.map(x => {
        //小时
        val hour = x._5
        //周几
        val week = x._4
        //日期:
        val date = x._3.split(" ")(0).replace("-", "")
        val s = if (hour >= 22 || hour <= 3) "夜猫子" else if (hour >= 3 && hour <= 7) "早起族" else "null"
        val we = if (week >= 1 && week <= 5 && hour >= 6 && hour <= 9) "上班族" else "null"
        val date_s = if (date_three_date_sc.value.contains(date)) date else "null"
        (x._1, (s, we, week + "", date_s))
      })
      res
    }).reduceByKey((x1, x2) => {
      val res = x1._1 + "\t" + x2._1
      val week = x1._2 + "\t" + x2._2
      val week_number = x1._3 + "\t" + x2._3
      val date = x1._4 + "\t" + x2._4
      (res, week, week_number, date)
    }).mapPartitions(rdd => {
      val jSONObject = new JSONObject
      val res = rdd.map(x => {
        //统计最近3个月的日期,从而求得骑行频率
        val riding_frequency = x._2._4.split("\t")
        val riding_frequency_res = if (riding_frequency.contains("null")) "null" else {
          val number = 90 / riding_frequency.length
          numberFormat.format(number)
        }

        //统计夜猫子和早起族出现的次数
        val y_count = x._2._1.split("\t").count(_ == "夜猫子")
        val z_count = x._2._1.split("\t").count(_ == "早起族")

        val zw = x._2._1.split("\t").mkString("|")
        //统计上班族出现的次数
        val week = x._2._2.split("\t").count(_ == "上班族")
        //找出对应的星期几:
        //主要骑行日期:周几
        val week_number = x._2._3.split("\t").map(x => (x, 1)).groupBy(_._1).map(x => (x._1, x._2.map(x => x._2).length)).reduce((x1, x2) => if (x1._2 >= x2._2) x1 else x2)._1
        val week_res = if (week_number.length > 0) week_number else "null"

        //统计在什么情况下我们判断他是夜猫子或者时早起族
        val rr = if (y_count > z_count && y_count >= 10) "夜猫子" else if (y_count < z_count && z_count >= 10) "早起族" else "null"

        //统计在什么情况下我们判断他是上班族
        val res_wek = if (week >= 3) "上班族" else "null"

        //如果,rr与res_wek，都有值则表示它既是夜猫子(早起族)同时也是上班族
        jSONObject.put("early_night", rr) //早起族还是上班族
        jSONObject.put("work", res_wek) //上班族
        jSONObject.put("weeks", week_res) //主要骑行日期
        jSONObject.put("frequency", riding_frequency_res) //骑行频率

        (x._1, jSONObject.toJSONString, "user_person_ofo_par")
        //              (x._1, s"$rr,$res_wek,主要骑行日期:$week_number,骑行频率:$riding_frequency_res", "user_person_ofo_par")
      })
      res
    })
    toHbase(user_person_ofo_par, columnFamily1, "user_person_ofo_par", conf_fs, tableName, conf)


    //    //主要骑行日期
    //    val user_most_riding_date_res = user_most_riding_date(open_ofo_policy_parquet: RDD[(String, String, String, Int, Int, Boolean, String)])
    //    toHbase(user_most_riding_date_res, columnFamily1, "user_most_riding_date", conf_fs, tableName, conf)
    //
    //    val total_time = open_ofo_policy_parquet.map(x => (x._1, x._5)).filter(x => if (x._2 >= 3 && x._2 <= 7 || x._2 >= 22 && x._2 <= 3) true else false)
    //
    //    //早起族
    //    val user_early_bird = total_time.filter(x => if (x._2 >= 3 && x._2 <= 7) true else false).map(x => (x._1, 1)).reduceByKey(_ + _).filter(_._2 >= 10).map(x => (x._1, "早起族", "user_early_bird"))
    //    toHbase(user_early_bird, columnFamily1, "user_early_bird", conf_fs, tableName, conf)
    //
    //    //夜猫子
    //    val user_night_bird = total_time.filter(x => if (x._2 >= 22 && x._2 <= 3) true else false).map(x => (x._1, 1)).reduceByKey(_ + _).filter(_._2 >= 10).map(x => (x._1, "夜猫子", "user_night_bird"))
    //    toHbase(user_night_bird, columnFamily1, "user_night_bird", conf_fs, tableName, conf)

    //    val user_early_bird = open_ofo_policy_parquet.map(x => {
    //      (x._1, x._5)
    //    }).filter(x => if (x._2 >= 3 && x._2 <= 7) true else false).map(x => (x._1, 1)).reduceByKey(_ + _).filter(_._2 >= 10).map(x => (x._1, "早起族", "user_early_bird"))
    //    toHbase(user_early_bird, columnFamily1, "user_early_bird", conf_fs, tableName, conf)

    //夜猫子
    //    val user_night_bird = open_ofo_policy_parquet.map(x => {
    //      (x._1, x._5)
    //    }).filter(x => if (x._2 >= 22 && x._2 <= 3) true else false).map(x => (x._1, 1)).reduceByKey(_ + _).filter(_._2 >= 10).map(x => (x._1, "夜猫子", "user_night_bird"))
    //    toHbase(user_night_bird, columnFamily1, "user_night_bird", conf_fs, tableName, conf)

    //    //上班族
    //    val user_on_worker = open_ofo_policy_parquet.map(x => (x._1, x._4, x._5)).filter(x => if (x._2 >= 1 && x._2 <= 5) true else false).filter(x => if (x._3 >= 6 && x._3 <= 9) true else false)
    //      .map(x => (x._1, 1)).reduceByKey(_ + _).filter(_._2 > 3).map(x => (x._1, "上班族", "user_on_worker"))
    //    toHbase(user_on_worker, columnFamily1, "user_on_worker", conf_fs, tableName, conf)

    //user_cycling_frequency:骑行频率
    //    val ss = getBeg_End().toArray
    //    val res = sc.broadcast(ss) //过滤hive分区
    //    val numberFormat = NumberFormat.getInstance
    //    numberFormat.setMaximumFractionDigits(2)
    //
    //    val user_cycling_frequency_r = open_ofo_policy_parquet.filter(x => if (res.value.contains(x._7)) true else false).map(x => (x._1, 1)).reduceByKey(_ + _).map(x => {
    //      val number = 90 / x._2
    //      (x._1, numberFormat.format(number), "user_cycling_frequency")
    //    })
    //    toHbase(user_cycling_frequency_r, columnFamily1, "user_cycling_frequency", conf_fs, tableName, conf)

  }

}
