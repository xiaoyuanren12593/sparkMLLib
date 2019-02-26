package bzn.job.label.goout

import java.text.SimpleDateFormat
import java.util.Date

import bzn.job.common.Until
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import sun.util.calendar.CalendarUtils.mod

/**
  * Created by MK on 2018/4/8.
  */
object UserNoneOfoTest extends Until {

  //各表查询
  def select_Table(sqlContext: HiveContext): (DataFrame) = {
    //接口平台-58保单数据 (这里我使用到了分区查，上集群的时候关闭)
    val open_express_policy_dedup = sqlContext.sql("select" +
      " courier_name," +
      " courier_mobile," +
      " courier_card_no," +
      " create_time," +
      " end_time," +
      " \"58\" as product_code," +
      " \"产品58\" as customer_Name," +
      " \"司机,速递\" as identity" +
      " from odsdb_prd.open_express_policy") // where day_id=20170503
      //      " from odsdb_prd.open_express_policy where day_id=20170503")
      .selectExpr("courier_card_no as cert", "courier_mobile as mobile", "create_time", "end_time", "product_code", "customer_Name", "identity", "courier_name as insured_name")

    //ods_policy_insured_detail:被保人清单级别信息总表
    val ods_policy_insured_detail = sqlContext.sql("select" +
      " insured_name," +
      " insured_mobile," +
      " insured_cert_no," +
      " insured_create_time," +
      " insured_end_date," +
      " \"官网\" as product_code," +
      " \"产品官网\" as customer_Name," +
      " insured_work_type as identity" +
      " from odsdb_prd.ods_policy_insured_detail")
      .selectExpr("insured_cert_no as cert", "insured_mobile as mobile", "insured_create_time as create_time", "insured_end_date as end_time", "product_code", "customer_Name", "identity", "insured_name")


    //open_other_policy	接口平台-其他保单
    val open_other_policy = sqlContext.sql("select" +
      " insured_name," +
      " insured_mobile," +
      " insured_cert_no," +
      " create_time," +
      " end_date," +
      " \"其他\" as product_code," +
      " \"产品其他\" as customer_Name," +
      " \"官网其他\" as identity" +
      " from odsdb_prd.open_other_policy") //where day_id=20170201
      //      " from odsdb_prd.open_other_policy where day_id=20170201")
      .selectExpr("insured_cert_no as cert", "insured_mobile as mobile", "create_time", "end_date as end_time", "product_code", "customer_Name", "identity", "insured_name")

    val sum: DataFrame = open_express_policy_dedup.unionAll(ods_policy_insured_detail).unionAll(open_other_policy)
    sum
  }

  def main(args: Array[String]): Unit = {
    val conf_s = new SparkConf().setAppName("wuYu")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
      .set("spark.sql.broadcastTimeout", "36000")
      .setMaster("local[2]")
    val sc = new SparkContext(conf_s)
    val sqlContext: HiveContext = new HiveContext(sc)
    val sum = select_Table(sqlContext: HiveContext)

    //d_constellation_1	星座码表
    val d_constellation_1: DataFrame = sqlContext.sql("select * from odsdb_prd.d_constellation_1").cache
    //d_cant	省市区码表	省市区信息
    val d_cant = sqlContext.sql("select * from odsdb_prd.d_cant").filter("length(name)>0").cache
    //d_city_grade	城市级别码表
    val d_city_grade: DataFrame = sqlContext.sql("select * from odsdb_prd.d_city_grade").cache

    val sum_before = sum.filter("length(cert)=18 and length(create_time) > 14 and length(end_time) > 14").map(x => {
      val date = x.getString(2)
      //创建投保日期
      val new_start_date = deletePoint(date).replace("/", "-")
      //终止日期
      val end_data_time = x.getString(3)
      val end_data_time_result = deletePoint(end_data_time).replace("/", "-")
      //保单号
      val code = x.getString(4)
      //产品所属表(自定义)
      val customer_Name = x.getString(5)
      // 产品状态(自定义)
      val identity = x.getString(6)
      //姓名
      val insured_name = x.getString(7)
      //身份证
      val cert = x.getString(0)
      //电话
      val mobile = x.getString(1)
      //年龄
      val age = cert.substring(6, 10)
      //得到年龄的前2位,并在后面进行过滤
      val age_before = cert.substring(6, 8)
      val cert_Eighteen = cert.substring(0, cert.length - 1)
      val result_sex: Boolean = cert_Eighteen.matches("[0-9]+") //得到身份证前18位，并判断是否为数字
      //得到省份编码
      val cert_native_province = cert.substring(0, 2) + "0000"
      //得到城市编码
      val cert_native_city = cert.substring(0, 4) + "00"
      //得到用户籍贯
      val cert_native_county = cert.substring(0, 6)
      //得到用户星座
      val cert_constellation = cert.substring(10, 14)

      (
        cert, mobile,
        currentTimeL(new_start_date), new_start_date,
        end_data_time_result, currentTimeL(end_data_time_result),
        code, customer_Name,
        identity, insured_name,
        age, age_before,
        result_sex, cert_native_province,
        cert_native_city, cert_native_county,
        cert_constellation
      )
    }).filter(x => {
      //这里将身份证前18位是数字的过滤出来，将出生年月中，是19年20年出生的过滤出来
      val result_sex = x._13
      val age_before = x._12
      if (result_sex && age_before == "19" || age_before == "20") true else false
    })

    /**
      * HBaseConf
      **/

    val conf = HbaseConf("labels:label_user_personal_vT")._1
    val conf_fs = HbaseConf("labels:label_user_personal_vT")._2
    val tableName = "labels:label_user_personal_vT"
    val columnFamily1 = "baseinfo"

    //得到省份
    val cert_native_province_r = d_cant.map(x => {
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

    //new日期对象
    val date = new Date
    //转换提日期输出格式
    val dateFormat = new SimpleDateFormat("yyyy")
    //去重
    val total = sum_before.map(x => {
      val cert = x._1
      val currentTimeL_new_start_date = x._3
      val new_start_date = x._4
      val end_data_time_result = x._5
      val currentTimeL_end_data_time_result = x._6
      val code = x._7
      val customer_Name = x._8
      val identity = x._9
      val insured_name = x._10
      //性别
      val sex = cert.substring(cert.length - 2, cert.length - 1).toInt
      val sex_res = mod(sex, 2)
      val end_woman_man = if (sex_res == 0) "女" else "男"
      //年龄
      val birthday = x._11.toInt
      val nowYear = getNowYear(date, dateFormat)
      val age = nowYear - birthday //年龄

      val cert_native_province = x._14
      val cert_native_city = x._15
      val cert_native_county = x._16
      val cert_constellation = x._17

      val cert_native_province_res = cert_native_province_r.getOrElse(cert_native_province, "0")
      val cert_native_city_res = cert_native_province_r.getOrElse(cert_native_city, "0")
      val cert_native_county_res = cert_native_province_r.getOrElse(cert_native_county, "0")
      val d_city_grade_res = d_city_grade_r.getOrElse(cert_native_city, "0")
      val cert_constellation_res = cert_constellation_r.getOrElse(cert_constellation, "0")
      //个人信息
      val sum = s"$insured_name,$age,$end_woman_man,$cert_native_province_res,$cert_native_city_res,$cert_native_county_res,$d_city_grade_res,$cert_constellation_res"
      (cert, (sum, currentTimeL_new_start_date, new_start_date, end_data_time_result, customer_Name, code, identity))
    }).distinct()

    //    val user_person = total.filter(_._2._2.toString.length > 10).map(x => {
    val user_person = total
      .mapPartitions(rdd => {
        val json: JSONObject = new JSONObject
        val res = rdd.map(x => {
          //Long时间
          val L_date = x._2._2
          //最早
          //投保的产品:
          val user_insure_product_before = x._2._6
          val user_insure_product = if (user_insure_product_before.length > 0) user_insure_product_before else "null"

          //投保至今月数
          val create_time = x._2._3.split(" ")(0)
          val res = getMonth(create_time)
          //最近
          //社会身份
          val identy = x._2._7
          val identy_res = if (identy.length > 0) identy else "null"

          //个人信息
          val information_before = x._2._1
          val information = if (information_before.length > 0) information_before else "null"

          //最后一次投保日期
          val end_before = x._2._3
          val end_data = if (end_before.length > 0) end_before else "null"

          //保险止期
          val b_x_before = x._2._4
          val b_x_date = if (b_x_before.length > 0) b_x_before else "null"

          //求总
          //保单号
          val code_before = x._2._6
          val code = if (code_before.length > 0) code_before else "null"
          json.put("time", L_date) //时间
          json.put("insurance_Products", user_insure_product) //投保的产品
          json.put("Insure_to_month", res) //投保至今月数
          json.put("social_Identity", identy_res) //社会身份
          json.put("user_information", information) //个人信息
          json.put("end_data", end_data) //最后一次投保日期
          json.put("b_x_date", b_x_date) //保险止期
          json.put("code", code) //保单号
          json.put("customer_Source", x._2._5) //客户来源
          json.put("start_date", create_time) //首次投保日期

          //cert | 时间     |投保的产品          | 投保至今月数 | 社会身份| 个人信息 | 最后一次投保日期 |保险止期 | 保单号 |客户来源 ,1
          //      (x._1, (s"$L_date~$user_insure_product~$res~$identy_res~$information~$end_data~$b_x_date~$code~${x._2._5}", 1))
          (x._1, (json.toJSONString, 1))
        })
        res
      })
      .reduceByKey((x1, x2) => {
        val total = x1._1 + "\t" + x2._1
        val num = x1._2 + x2._2
        (total, num)
      })
      .mapPartitions(rdd => {
        val jSONObject = new JSONObject
        val jsonObject_value = new JSONObject
        val jsonObject_value_before = new JSONObject
        val jsonObject_value_before_total = new JSONObject
        val res = rdd.map(x => {
          //Long时间戳和total数据
          val date_long = x._2._1.split("\t").map(x => (JSON.parseObject(x).getLong("time"), JSON.parseObject(x)))
          //求得最近的值
          val nearestValue = date_long.reduce((x1, x2) => if (x1._1 >= x2._1) x1 else x2)._2
          jsonObject_value.put("user_information", nearestValue.getString("user_information"))
          jsonObject_value.put("end_data", nearestValue.getString("end_data"))
          jsonObject_value.put("b_x_date", nearestValue.getString("b_x_date"))
          jsonObject_value.put("social_Identity", nearestValue.getString("social_Identity"))


          //求得最早的值
          val earliest = date_long.reduce((x1, x2) => if (x1._1 <= x2._1) x1 else x2)._2
          jsonObject_value_before.put("Insure_to_month", earliest.getString("Insure_to_month"))
          jsonObject_value_before.put("insurance_Products", earliest.getString("insurance_Products"))
          jsonObject_value_before.put("customer_Source", earliest.getString("customer_Source"))
          jsonObject_value_before.put("start_date", earliest.getString("start_date"))


          //user_insure_code:用户产品号
          val user_insure_code = x._2._1.split("\t").map(x => JSON.parseObject(x).getString("code")).distinct.mkString("|")
          //user_insure_product_num:投保产品数
          val user_insure_product_num = x._2._2 + ""

          jsonObject_value_before_total.put("user_insure_code", user_insure_code)
          jsonObject_value_before_total.put("user_insure_product_num", user_insure_product_num)

          //汇总
          jSONObject.put("personal_Information", jsonObject_value)
          jSONObject.put("fist_now_month_and_product", jsonObject_value_before)
          jSONObject.put("user_insure_code_num", jsonObject_value_before_total)

          (x._1, jSONObject.toJSONString, "user_person_none_ofo")
          //        (x._1, jsonObject_value, jsonObject_value_before, jsonObject_value_before_total)
        })
        res
      })
    user_person.take(10).foreach(x => println(x))

    //    saveToHbase(user_person, columnFamily1, "user_person_none_ofo", conf_fs, tableName, conf)
  }
}
