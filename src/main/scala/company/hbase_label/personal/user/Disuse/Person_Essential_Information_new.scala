package company.hbase_label.personal.user.Disuse

import java.text.SimpleDateFormat
import java.util.Date

import company.hbase_label.until
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import sun.util.calendar.CalendarUtils.mod

/**
  * Created by MK on 2018/4/8.
  */
object Person_Essential_Information_new extends until {


  //user_insure_coverage:骑行保障情况
  def user_insure_coverage(total: RDD[(String, (String, Long, String, String, String, String, String))], sqlContext: HiveContext, before: DataFrame): RDD[(String, String, String)]
  = {
    val bz = total.filter(_._2._5 == "产品ofo").map(x => {
      val res = if (x._2._6 == "OFO00002") 50 + "" else 30 + ""
      (x._1, res)
    })
    val after = sqlContext.sql("select * from odsdb_prd.ods_policy_insured_detail").filter("LENGTH(insured_cert_no)=18").select("policy_id", "insured_cert_no")
    val j_after: RDD[(String, String)] = before.join(after, "policy_id").select("insured_cert_no", "sku_coverage").map(x => (x.getString(0), x.getString(1))).reduceByKey((x1, x2) => {
      val res = if (x1 != x2) x1 else x1
      res
    })
    val end = j_after.union(bz).map(x => (x._1, x._2, "user_insure_coverage"))
    end
  }

  //各表查询
  def select_Table(sqlContext: HiveContext): (DataFrame)
  = {
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

    //open_ofo_policy:open_ofo_policy	接口平台-ofo数据
    val open_ofo_policy = sqlContext.sql("select " +
      "insured_name," +
      " insured_mobile," +
      " insured_cert_no," +
      " start_date," +
      " end_date," +
      " product_code," +
      " \"产品ofo\" as customer_Name," +
      " \"其他\" as identity" +
      " from odsdb_prd.open_ofo_policy_parquet") //where day_id=20160901
      //      " from odsdb_prd.open_ofo_policy where day_id=20160901")
      .selectExpr("insured_cert_no as cert", "insured_mobile as mobile", "start_date as create_time", "end_date as end_time", "product_code", "customer_Name", "identity", "insured_name")

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
      " from odsdb_prd.open_other_policy ") //where day_id=20170201
      //      " from odsdb_prd.open_other_policy where day_id=20170201")
      .selectExpr("insured_cert_no as cert", "insured_mobile as mobile", "create_time", "end_date as end_time", "product_code", "customer_Name", "identity", "insured_name")

    val sum: DataFrame = open_express_policy_dedup.unionAll(open_ofo_policy).unionAll(ods_policy_insured_detail).unionAll(open_other_policy)
    sum
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf_s = new SparkConf().setAppName("wuYu")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
      .set("spark.sql.broadcastTimeout", "36000")
    //      .setMaster("local[2]")
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


    /**
      * 每天执行该操作的时候，都删除一次昨天的数据
      **/

    //    truncate_hbase(sc, tableName)

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
    })

    //去重
    val total_dinstinct = total.map(x => (x, 1)).reduceByKey((x, y) => x).map(_._1)

    //取得最近的投保日期
    val total_after = total_dinstinct.reduceByKey((x1, x2) => if (x1._2 >= x2._2) x1 else x2)

    //取得最早的投保日期
    val total_before = total_dinstinct.reduceByKey((x1, x2) => if (x1._2 <= x2._2) x1 else x2)

    //user_fist_now_month:首次投保至今月数|user_fist_insure_product:首次投保产品 | 客户来源
    val fist_now_month_and_product = total_before.map(x => {
      //首次投保产品
      val user_fist_insure_product = x._2._6
      val cert = x._1
      val create_time = x._2._3.split(" ")(0)
      val res = getMonth(create_time)
      (cert, s"首次投保至今月数:$res,首次投保产品$user_fist_insure_product,客户来源:${x._2._5}", "fist_now_month_and_product")
    })
    toHbase(fist_now_month_and_product, columnFamily1, "fist_now_month_and_product", conf_fs, tableName, conf)


    //个人信息|最后一次投保日期|保险止期 | 社会身份(OK)
    val personal_Information = total_after.map(x => {
      val identy = x._2._7
      val res = if (identy.length > 0) identy else "null"
      (x._1, s"个人信息:${x._2._1},最后一次投保日期:${x._2._3},保险止期:${x._2._4},社会身份:$res", "Personal_Information")
    })
    toHbase(personal_Information, columnFamily1, "Personal_Information", conf_fs, tableName, conf)


    //  user_insure_code:用户产品号 |user_insure_product_num:投保产品数 (ok)  OFO://用户类型 | 骑行天数 |骑行保障 | 骑行品牌(ok)
    val user_insure_code_num = total_dinstinct.map(x => {
      //cert | code | 1 |
      (x._1, (x._2._6, 1))
    }).reduceByKey((x1, x2) => {
      val code = x1._1 + "\t" + x2._1
      val number = x1._2 + x2._2
      (code, number)
    }).map(x => {
      //user_insure_code:用户产品号
      val user_insure_code = x._2._1.split("\t").distinct.mkString("|")
      //user_insure_product_num:投保产品数
      val num = x._2._2.toString
      //用户类型 | 骑行天数 |骑行保障 | 骑行品牌(ok)
      val product_s: Array[String] = x._2._1.split("\t")
      val version = if (product_s.contains("OFO00001") && !product_s.contains("OFO00002")) s"用户类型:校园版,骑行天数:${count_num(product_s, "OFO00001")},保障额:30,骑行品牌:ofo"
      else if (product_s.contains("OFO00002") && !product_s.contains("OFO00001")) s"用户类型:城市版,骑行天数:${count_num(product_s, "OFO00002")},保障额:50,骑行品牌:ofo"
      else if (product_s.contains("OFO00001") && product_s.contains("OFO00002")) s"该用户有同时使用过城市版本和校园版本,骑行天数:${count_num(product_s, "OFO00001") + count_num(product_s, "OFO00002")},保障额:城市版50校园版30,骑行品牌:ofo" else "非OFO"

      (x._1, s"用户产品号:$user_insure_code,投保产品数:$num,$version", "user_insure_code_num")
    })
    toHbase(user_insure_code_num, columnFamily1, "user_insure_code_num", conf_fs, tableName, conf)

    //    //ofo
    //    //用户类型 | 骑行天数 |骑行保障 | 骑行品牌(ok)
    //    val user_type_days = total_sum.map(x => {
    //      val product_s: Array[String] = x._2._1.split("\t")
    //      val version = if (product_s.contains("OFO00001") && !product_s.contains("OFO00002")) s"用户类型:校园版,骑行天数:${count_num(product_s, "OFO00001")},保障额:30,骑行品牌:ofo"
    //      else if (product_s.contains("OFO00002") && !product_s.contains("OFO00001")) s"用户类型:城市版,骑行天数:${count_num(product_s, "OFO00002")},保障额:50,骑行品牌:ofo"
    //      else if (product_s.contains("OFO00001") && product_s.contains("OFO00002")) s"该用户有同时使用过城市版本和校园版本,骑行天数:${count_num(product_s, "OFO00001") + count_num(product_s, "OFO00002")},保障额:城市版50校园版30,骑行品牌:ofo" else "非OFO"
    //      (x._1, s"$version", "user_type_days_sku")
    //    }).filter(_._2 != "0")
    //    toHbase(user_type_days, columnFamily1, "user_type_days", conf_fs, tableName, conf)


  }
}
