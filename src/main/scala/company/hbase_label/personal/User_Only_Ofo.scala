package company.hbase_label.personal

import java.text.{NumberFormat, SimpleDateFormat}
import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.util.Date

import com.alibaba.fastjson.JSONObject
import company.hbase_label.until
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import sun.util.calendar.CalendarUtils.mod

/**
  * Created by a2589 on 2018/4/3.
  */

object User_Only_Ofo extends until {

  //个人信息
  def user_Information(date: Date, dateFormat: SimpleDateFormat, x: (String, (String, String, String, String, String, Long, String, String, String, String, Int)), cert_native_province_r: collection.Map[String, String], d_city_grade_r: collection.Map[String, String], cert_constellation_r: collection.Map[String, String]): String
  = {
    //个人信息
    //得到省份编码
    val cert_native_province = x._1.substring(0, 2) + "0000"
    //得到城市编码
    val cert_native_city = x._1.substring(0, 4) + "00"
    //得到用户籍贯
    val cert_native_county = x._1.substring(0, 6)
    //得到用户星座
    val cert_constellation = x._1.substring(10, 14)

    val cert_native_province_res = cert_native_province_r.getOrElse(cert_native_province, "0")
    val cert_native_city_res = cert_native_province_r.getOrElse(cert_native_city, "0")
    val cert_native_county_res = cert_native_province_r.getOrElse(cert_native_county, "0")
    val d_city_grade_res = d_city_grade_r.getOrElse(cert_native_city, "0")
    val cert_constellation_res = cert_constellation_r.getOrElse(cert_constellation, "0")
    //名字
    val insured_name = x._2._5.split("\t").distinct.mkString("|")

    //年龄
    val birthday = x._1.substring(6, 10).toInt
    val nowYear = getNowYear(date, dateFormat)
    val age = nowYear - birthday //年龄

    //性别
    val sex = x._1.substring(x._1.length - 2, x._1.length - 1).toInt
    val sex_res = mod(sex, 2)
    val end_woman_man = if (sex_res == 0) "女" else "男"
    //个人信息
    val sum = s"$insured_name,$age,$end_woman_man,$cert_native_province_res,$cert_native_city_res,$cert_native_county_res,$d_city_grade_res,$cert_constellation_res"
    sum
  }


  //上班族，早起和夜猫子，骑行频率，主要骑行日期
  def ofo_information(x: (String, (String, String, String, String, String, Long, String, String, String, String, Int)), numberFormat: NumberFormat): (String, String, String, String)
  = {
    //统计最近3个月的日期,从而求得骑行频率
    val riding_frequency = x._2._4.split("\t")
    val riding_frequency_res = if (riding_frequency.contains("null")) "null" else {
      val number = 90 / riding_frequency.length
      numberFormat.format(number)
    }
    //统计夜猫子和早起族出现的次数
    val y_count = x._2._1.split("\t").count(_ == "夜猫子")
    val z_count = x._2._1.split("\t").count(_ == "早起族")

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
    (rr, res_wek, week_res, riding_frequency_res)

  }


  def user_ofo_only(x: (String, (String, String, String, String, String, Long, String, String, String, String, Int))): (String, String, String, String, String, Int, (String, String, String))
  = {
    //求得最近的值
    //最后一次投保日期
    val end_date = x._2._7
    //最后一次保险止期
    val b_x_date = x._2._8
    //求得最早的
    //首次投保日期
    val start_date = x._2._9
    //首次投保产品(也是用户产品号)
    val start_product = x._2._10
    //首次投保至今月数
    val create_time = start_date.split(" ")(0)
    val res = getMonth(create_time)
    //投保的次数
    val number_product = x._2._11
    //最近一次骑行的类型
    val version = if (start_product == "OFO00001") ("校园版", "30", "ofo")
    else if (start_product == "OFO00002") ("城市版", "50", "ofo") else ("null", "null", "非ofo")

    (end_date, b_x_date, start_date, start_product, res, number_product, version)

  }

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


    //d_constellation_1	星座码表
    val d_constellation_1: DataFrame = sqlContext.sql("select * from odsdb_prd.d_constellation_1").cache
    //d_cant	省市区码表	省市区信息
    val d_cant = sqlContext.sql("select * from odsdb_prd.d_cant").filter("length(name)>0").cache
    //d_city_grade	城市级别码表
    val d_city_grade: DataFrame = sqlContext.sql("select * from odsdb_prd.d_city_grade").cache

    //得到省份
    val cert_native_province_r: collection.Map[String, String] = d_cant.map(x => {
      val code = x.getString(0)
      val name = x.getString(1)
      (code, name)
    }).distinct().collectAsMap
    //得到星座
    val cert_constellation_r = d_constellation_1.map(x => {
      (x.getString(1), x.getString(2))
    }).distinct().collectAsMap
    //城市级别
    val d_city_grade_r = d_city_grade.map(x => {
      (x.get(3) + "", x.get(2) + "")
    }).distinct().collectAsMap


    val open_ofo_policy_parquet = sqlContext
      .sql("select * from odsdb_prd.open_ofo_policy_parquet")
      //      .sql("select * from odsdb_prd.open_ofo_policy_parquet where day_id in(20180223,20180222,20161201)")
      .filter("length(insured_cert_no) =18  and length(start_date)>14 and length(end_date) > 14")
      .map(x => {
        val formatter_before = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        val qs_hs = new DateTimeFormatterBuilder().append(formatter_before).toFormatter()


        val cert = x.getAs("insured_cert_no").toString
        val start_date = x.getAs("start_date").toString.replaceAll("/", "-")
        val de_start_date = deletePoint(start_date)
        val cert_Eighteen = cert.substring(0, cert.length - 1)
        val result_sex: Boolean = cert_Eighteen.matches("[0-9]+") //得到身份证前18位，并判断是否为数字

        //终止日期
        val end_data_time = x.getAs("end_date").toString
        val end_data_time_result = deletePoint(end_data_time).replace("/", "-")


        //得到今天是星期几
        val week_num = getWeekOfDate(de_start_date.split(" ")(0))
        //保单号
        val product_code = x.getAs("product_code").toString
        //得到日期的小时
        val lod_before = LocalDateTime.parse(de_start_date, qs_hs)
        val hour = lod_before.getHour
        val name = if (x.getAs("insured_name") != null) x.getAs("insured_name").toString else "null"

        //得到年龄的前2位,并在后面进行过滤
        val age_before = cert.substring(6, 8)
        val age = if (age_before == "19" || age_before == "20") cert.substring(6, 10) else "0"


        (cert, product_code, de_start_date, week_num, hour, result_sex, x.getAs("day_id").toString, name, age, currentTimeL(de_start_date), de_start_date, end_data_time_result)
      }).filter(x => if (x._6 && x._9 != "0") true else false)

    val numberFormat: NumberFormat = NumberFormat.getInstance
    numberFormat.setMaximumFractionDigits(2)

    //HBaseConf
    val conf = HbaseConf("labels:label_user_personal_vT")._1
    val conf_fs = HbaseConf("labels:label_user_personal_vT")._2
    val tableName = "labels:label_user_personal_vT"
    val columnFamily1 = "goout"

    //得到最近3个月的所有日期
    val date_three_date = getBeg_End().toArray
    val date_three_date_sc = sc.broadcast(date_three_date)
    //早起族|夜猫子|上班族|主要骑行日期|骑行频率 ...
    val user_person_ofo_par: RDD[(String, String, String)] = open_ofo_policy_parquet.mapPartitions(rdd => {
      val res = rdd.map(x => {
        //创建投保日期Long
        val start_date_Long = x._10
        //创建投保日期
        val start_date = x._11
        //创建终止日期
        val end_date = x._12
        //保单号类型
        val code = if (x._2.length > 0) x._2 else "null"
        val name = x._8
        //小时
        val hour = x._5
        //周几
        val week = x._4
        //日期:
        val date = x._3.split(" ")(0).replace("-", "")
        val s = if (hour >= 22 || hour <= 3) "夜猫子" else if (hour >= 3 && hour <= 7) "早起族" else "null"
        val we = if (week >= 1 && week <= 5 && hour >= 6 && hour <= 9) "上班族" else "null"
        val date_s = if (date_three_date_sc.value.contains(date)) date else "null"
        (x._1, (s, we, week + "", date_s, name, start_date_Long, start_date, end_date, start_date, code, 1))
      })
      res
    }).reduceByKey((x1, x2) => {
      val res = x1._1 + "\t" + x2._1
      val week = x1._2 + "\t" + x2._2
      val week_number = x1._3 + "\t" + x2._3
      val date = x1._4 + "\t" + x2._4
      val name = x1._5 + "\t" + x2._5
      //最近的日期L，最后一次投保日期，最后一次保险止期
      val date_after = if (x1._6 >= x2._6) (x1._6, x1._7, x1._8) else (x2._6, x2._7, x2._8)

      //首次投保日期 | 首次投保的产品
      val date_before = if (x1._6 <= x2._6) (x1._9, x1._10) else (x2._9, x2._10)

      //投保了几次
      val number = x1._11 + x2._11
      (res, week, week_number, date, name, date_after._1, date_after._2, date_after._3, date_before._1, date_before._2, number)
    }).mapPartitions(rdd => {
      val jSONObject = new JSONObject
      //new日期对象
      val date: Date = new Date
      //转换提日期输出格式
      val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy")
      val res = rdd.map(x => {
        /**
          * 求得最近的值
          **/
        //最后一次投保日期
        val end_date = user_ofo_only(x)._1
        //最后一次保险止期
        val b_x_date = user_ofo_only(x)._2
        /**
          * 求得最早的
          **/
        //首次投保日期
        val start_date = user_ofo_only(x)._3
        //首次投保产品(也是用户产品号)
        val start_product = user_ofo_only(x)._4
        //首次投保至今月数
        val res = user_ofo_only(x)._5

        //投保的次数
        val number_product = user_ofo_only(x)._6
        //最近一次骑行的类型
        val version = user_ofo_only(x)._7


        //个人信息
        val sum = user_Information(date, dateFormat, x, cert_native_province_r, d_city_grade_r, cert_constellation_r)

        //上班族，早起和夜猫子，骑行频率，主要骑行日期
        val rr = ofo_information(x, numberFormat)._1
        val res_wek = ofo_information(x, numberFormat)._2
        val week_res = ofo_information(x, numberFormat)._3
        val riding_frequency_res = ofo_information(x, numberFormat)._4


        //如果,rr与res_wek，都有值则表示它既是夜猫子(早起族)同时也是上班族
        jSONObject.put("early_night", rr) //早起族还是上班族
        jSONObject.put("work", res_wek) //上班族
        jSONObject.put("weeks", week_res) //主要骑行日期
        jSONObject.put("frequency", riding_frequency_res) //骑行频率

        jSONObject.put("user_information", sum) //个人信息
        jSONObject.put("end_data", end_date) //最近一次投保的日期
        jSONObject.put("b_x_date", b_x_date) //最近一次投完保后的止期

        jSONObject.put("start_date", start_date) //首次投保日期
        jSONObject.put("Insure_to_month", res) //首次投保至今的月份
        jSONObject.put("insurance_Products", start_product) //首次投保的产品号
        jSONObject.put("customer_Source", "ofo") //客户来源

        jSONObject.put("user_insure_product_num", number_product) //投保产品数

        jSONObject.put("user_type", version._1)
        jSONObject.put("ofo_guarantee", version._2)
        jSONObject.put("ofo_brand", version._3)
        (x._1, jSONObject.toJSONString, "user_person_only_ofo")
      })
      res
    })
    toHbase(user_person_ofo_par, columnFamily1, "user_person_only_ofo", conf_fs, tableName, conf)

  }

}
