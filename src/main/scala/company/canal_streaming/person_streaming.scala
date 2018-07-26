package company.canal_streaming

import java.text.{NumberFormat, SimpleDateFormat}
import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import company.hbase_label.personal.Cbaseinfo.toHbase
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import sun.util.calendar.CalendarUtils.mod


/**
  * Created by MK on 2018/6/6.
  */
object person_streaming extends hbaseUntil {


  val conf_hbase = HbaseConf("copy_m")._1
  val conf_fs_hbase = HbaseConf("copy_m")._2
  val tableName = "copy_m"
  val columnFamily1 = "infor1"



  //个人信息
  def user_Information(date: Date,
                       dateFormat: SimpleDateFormat,
                       x: (String, (String, String, String, String, String, Long, String, String, String, String, Int)),
                       cert_native_province_r: collection.Map[String, String],
                       d_city_grade_r: collection.Map[String, String],
                       cert_constellation_r: collection.Map[String, String]): String
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

  //得到hive中的各个表
  def getTable(hiveContext: HiveContext): (collection.Map[String, String], collection.Map[String, String], collection.Map[String, String])
  = {
    //d_constellation_1	星座码表
    val d_constellation_1: DataFrame = hiveContext.sql("select * from odsdb_prd.d_constellation_1").cache
    //d_cant	省市区码表	省市区信息
    val d_cant = hiveContext.sql("select * from odsdb_prd.d_cant").filter("length(name)>0").cache
    //d_city_grade	城市级别码表
    val d_city_grade: DataFrame = hiveContext.sql("select * from odsdb_prd.d_city_grade").cache

    //得到省份
    val cert_native_province_r: collection.Map[String, String] = d_cant.map(x => {
      val code = x.getString(0)
      val name = x.getString(1)
      (code, name)
    }).distinct().collectAsMap
    //得到星座
    val cert_constellation_r = d_constellation_1.map(x => (x.getString(1), x.getString(2))).distinct().collectAsMap
    //城市级别
    val d_city_grade_r = d_city_grade.map(x => (x.get(3) + "", x.get(2) + "")).distinct().collectAsMap
    (cert_native_province_r, cert_constellation_r, d_city_grade_r)
  }

  //对累加后的数据进行计算
  def get_leijia(d_city_grade_r: collection.Map[String, String], cert_constellation_r: collection.Map[String, String], cert_native_province_r: collection.Map[String, String], tep_three: DStream[(String, (String, String, String, String, String, Long, String, String, String, String, Int))], numberFormat: NumberFormat)
  : DStream[(String, String, String)] = {
    val end: DStream[(String, String, String)] = tep_three.reduceByKey((x1, x2) => {
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
        jSONObject.put("early_night", rr) //早起族还是夜猫子
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

        jSONObject.put("user_insure_product_num", number_product) //投保次数

        jSONObject.put("user_type", version._1) //城市版还是校园版
        jSONObject.put("ofo_guarantee", version._2)
        jSONObject.put("ofo_brand", version._3)
        (x._1, jSONObject.toJSONString, "user_person_only_ofo")
      })
      res
    })
    end

  }

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")

    /*
      * init sparkStream couchbase kafka
      **/
    val conf = new SparkConf().setAppName("CouchbaseKafka")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint("hdfs://namenode1.cdh:8020/model_data/check")

    val hiveContext: HiveContext = new HiveContext(sc)
    hiveContext.sql("set hive.exec.dynamic.partition.mode = nonstrict")
    //得到省份
    val cert_native_province_r: collection.Map[String, String] = getTable(hiveContext)._1
    //得到星座
    val cert_constellation_r: collection.Map[String, String] = getTable(hiveContext)._2
    //城市级别
    val d_city_grade_r: collection.Map[String, String] = getTable(hiveContext)._3

    /*
      * kafka conf
      **/
    val kafkaParam: Map[String, String] = Map[String, String](
      //-----------kafka低级api配置-----------
      "zookeeper.connect" -> "namenode2.cdh:2181,datanode3.cdh:2181,namenode1.cdh:2181", //----------配置zookeeper-----------
      "metadata.broker.list" -> "namenode1.cdh:9092",
      "group.id" -> "canal_kafka", //设置一下group id
      "auto.offset.reset" -> kafka.api.OffsetRequest.LargestTimeString, //----------从该topic最新的位置开始读数------------
      "client.id" -> "canal_kafka",
      "zookeeper.connection.timeout.ms" -> "10000"
    )

    //    val topicSet: Set[String] = Set("canal_update_state_by_key")
    val topicSet: Set[String] = Set("test-kevin")
    val directKafka: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topicSet)
    val lines: DStream[(String, String)] = directKafka.map((x: (String, String)) => (x._1, x._2)) // kafka取出的数据，_1是其topic，_2是消息
    //    (bzn_open_201806-open_ofo_policy-1529473761325,{"end_date":"2018-06-20 23:59:59","insured_sex":"","dataBase":"bzn_open_201806","export_status":"0","policy_id":"1256aad62cd0478da7d48dd2c3594990","extend_key5":"","batch_id":"","extend_key4":"","extend_key3":"","extend_key2":"","extend_key1":"","holder_birth_day":"","product_code":"OFO00001","insured_industry":"","holder_email":"","tableName":"open_ofo_policy","holder_last_name":"","update_time":"2018-06-20 07:42:33","insured_name":"","holder_ename":"","start_date":"2018-06-20 07:40:18","holder_name":"","proposal_no":"OFO180620861c4cc8bd3704efb5dba0cf","policy_no":"","insured_cert_type":"1","create_time":"2018-06-20 07:42:33","holder_first_name":"","insured_ename":"","holder_cert_type":"","eventTypes":"INSERT","insured_mobile":"18032868330","holder_sex":"","insured_first_name":"","holder_industry":"","insured_last_name":"","month":"2018-06-01","user_id":"100000002","insured_cert_no":"","holder_mobile":"","holder_cert_no":"","insured_email":"empty","insured_holder_relation":"","order_id":"2729187-1900082111","insured_birth_day":"","status":"1"})

    //    lines.filter(_._1.contains("open_ofo_policy"))
    //      .foreachRDD(rdd => rdd.foreach(x => println("过滤前", x)))

    val numberFormat: NumberFormat = NumberFormat.getInstance
    numberFormat.setMaximumFractionDigits(2)
    //得到最近3个月的所有日期
    val date_three_date = getBeg_End().toArray
    val date_three_date_sc = sc.broadcast(date_three_date)

    val addFunc = (currValues: Seq[String], prevValueState: Option[String]) => {
      //通过Spark内部的reduceByKey按key规约，然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
      //      prevValueState
      val previousCount = prevValueState.getOrElse(0)
      val ss = currValues.mkString(",")
      Some(ss + ";" + previousCount)
    }

    val open_ofo_policy_parquet = lines.filter(_._1.contains("open_ofo_policy")).map(x => JSON.parseObject(x._2)).filter(x => x.getString("insured_cert_no").length == 18 && x.getString("start_date").length > 14 && x.getString("end_date").length > 14)
      .map(x => {
        val formatter_before = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        val qs_hs = new DateTimeFormatterBuilder().append(formatter_before).toFormatter()


        val cert = x.getString("insured_cert_no")
        val start_date = x.getString("start_date").replaceAll("/", "-")
        val de_start_date = deletePoint(start_date)
        val cert_Eighteen = cert.substring(0, cert.length - 1)
        val result_sex: Boolean = cert_Eighteen.matches("[0-9]+") //得到身份证前18位，并判断是否为数字

        //终止日期
        val end_data_time = x.getString("end_date")
        val end_data_time_result = deletePoint(end_data_time).replace("/", "-")


        //得到今天是星期几
        val week_num = getWeekOfDate(de_start_date.split(" ")(0))
        //保单号
        val product_code = x.getString("product_code")
        //得到日期的小时
        val lod_before = LocalDateTime.parse(de_start_date, qs_hs)
        val hour = lod_before.getHour
        val name = if (x.getString("insured_name") != null) x.getString("insured_name") else "null"
        //得到年龄的前2位,并在后面进行过滤
        val age_before = cert.substring(6, 8)
        val age = if (age_before == "19" || age_before == "20") cert.substring(6, 10) else "0"
        (cert, product_code, de_start_date, week_num, hour, result_sex, x.getString("day_id"), name, age, currentTimeL(de_start_date), de_start_date, end_data_time_result)
      }).filter(x => if (x._6 && x._9 != "0") true else false)
    //早起族|夜猫子|上班族|主要骑行日期|骑行频率 ...
    val endd_final = open_ofo_policy_parquet.mapPartitions(rdd => {
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
        (x._1, s"${x._1},$s,$we,$week,$date_s,$name,$start_date_Long,$start_date,$end_date,$start_date,$code,1")
      })
      res
    })
    val tep_three: DStream[(String, (String, String, String, String, String, Long, String, String, String, String, Int))] = endd_final.updateStateByKey[String](addFunc).flatMap(x => {
      x._2.split(";")
    }).filter(_.length > 3).map(x => {
      val tep_one = x.split(",")
      (tep_one(0), (tep_one(1), tep_one(2), tep_one(3), tep_one(4), tep_one(5), tep_one(6).toLong, tep_one(7), tep_one(8), tep_one(9), tep_one(10), 1))
    })
    val end_final = get_leijia(d_city_grade_r, cert_constellation_r, cert_native_province_r, tep_three, numberFormat)


    end_final.foreachRDD(rdd => {
      toHbase(rdd, columnFamily1, "user_person_only_ofo", conf_fs_hbase, tableName, conf_hbase)
    })
    ssc.start()
    ssc.awaitTermination()
  }


}
