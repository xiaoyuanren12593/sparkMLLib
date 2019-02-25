package bzn.job.label.ent_insureinfo

import java.text.{NumberFormat, SimpleDateFormat}
import java.util.Properties
import java.util.regex.Pattern

import bzn.job.label.ent_claiminfo.ClaiminfoUntil
import bzn.job.label.ent_insureinfo.InsureinfoUntil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object EntInsureinfoTest extends InsureinfoUntil with ClaiminfoUntil {

  def is_not_chinese(str: String): Boolean
  = {
    val p = Pattern.compile("[\u4e00-\u9fa5]")
    val m = p.matcher(str)
    m.find()
  }

  def is_not_date(str: String): Boolean
  = {
    var convertSuccess: Boolean = true
    // 指定日期格式为四位年/两位月份/两位日期，注意yyyy/MM/dd区分大小写；
    var format = new SimpleDateFormat("yyyy/MM/dd")
    // 设置lenient为false. 否则SimpleDateFormat会比较宽松地验证日期，比如2007/02/29会被接受，并转换成2007/03/01
    try {
      format.setLenient(false);
      format.parse(str)
    } catch {
      case e => convertSuccess = false
    };
    convertSuccess
  }

  def Insure(sqlContext: HiveContext, location_mysql_url: String, location_mysql_url_dwdb: String, prop: Properties) {
    //    employer_liability_claims :雇主保理赔记录表
    //            意思是：
    //              也可以理解为出险表，可以这样想，我生病了去看病，要报销，这就可以是一个数据
    //    ods_policy_detail：保单表
    //    dim_product	企业产品信息表
    val ods_policy_detail: DataFrame = sqlContext.sql("select * from odsdb_prd.ods_policy_detail")
      .cache()
    //被保人明细表
    val ods_policy_insured_detail = sqlContext.sql("select * from odsdb_prd.ods_policy_insured_detail")
    //保全明细表
    val d_work_level = sqlContext.sql("select * from odsdb_prd.d_work_level").cache()
    //求出该企业中第一工种出现的类型哪个最多
    val ent_first_craft_data = ent_first_craft(ods_policy_insured_detail, ods_policy_detail, d_work_level)
    ent_first_craft_data.take(10).foreach(x => {
      printf(s"第一工种  ent_id=${x._1}, 工种=${x._2}, 字段名称=${x._3}\n")
    })

    //求出该企业中第二工种出现的类型哪个最多
    val ent_second_craft_data = ent_second_craft(ods_policy_insured_detail, ods_policy_detail, d_work_level).distinct()
    ent_second_craft_data.take(10).foreach(x => {
      printf(s"第二工种  ent_id=${x._1}, 工种=${x._2}, 字段名称=${x._3}\n")
    })

    //求出该企业中第三工种出现的类型哪个最多
    val ent_third_craft_data = ent_third_craft(ods_policy_insured_detail, ods_policy_detail, d_work_level).distinct()
    ent_third_craft_data.take(10).foreach(x => {
      printf(s"第三工种  ent_id=${x._1}, 工种=${x._2}, 字段名称=${x._3}\n")
    })
  }

  def ent_report_time(employer_liability_claims_r: DataFrame, ods_policy_detail_r: DataFrame): RDD[(String, String, String)] = {
    //report_date:报案日期
    //risk_date:出险日期

    val numberFormat = NumberFormat.getInstance
    numberFormat.setMaximumFractionDigits(2)
    val employer_liability_claims = employer_liability_claims_r.filter("length(report_date)>3 and length(risk_date)>3").select("policy_no", "report_date", "risk_date")
      //    val employer_liability_claims = sqlContext.sql("select lc.policy_no, lc.report_date ,lc.risk_date from  odsdb_prd.employer_liability_claims lc")
      //      .filter("length(report_date)>3 and length(risk_date)>3")
      .map(x => x).filter(x => {
      val date3 = x.getString(1)
      val date3_ = x.getAs[String]("report_date")
      val date4_ = x.getAs[String]("risk_date")
      val date4 = x.getString(2)
      if (!is_not_chinese(date3) && !is_not_chinese(date4)) {
        if (is_not_date(date3) && is_not_date(date4)) {
          true
        } else {
          false
        }
      } else {
        false
      }
    }).map(x => {
      val date3 = x.getString(1)
      val date4 = x.getString(2)
      println(date3 + "        " + date4)
      val day_mix: Int = xg(date3, date4)
      //日期相同的话，日期相减是0，因此如果是0的话，那么我们返回1天来进行计算，
      val dm = if (day_mix == 0) 1 else day_mix
      //保单号，出险与保险相差的天数(报>=出)
      (x.getString(0), dm)
    })
    val ods_policy_detail = ods_policy_detail_r.select("ent_id", "policy_code")
      //    val ods_policy_detail = sqlContext.sql("select pd.ent_id,pd.policy_code from odsdb_prd.ods_policy_detail pd")
      .map(x => {
      //police_code | ent_id
      (x.getString(1), x.getString(0))
    })
    //根据保单ID做一个Join，找到我对应的在保单ID中的数据(企业ent_id在保单表中)
    //ent_id，小时(存到了hbase中的ent_report_time)
    val end_result: RDD[(String, String, String)] = employer_liability_claims.join(ods_policy_detail).map(x => {
      val police_id = x._1 //police_id
      val day_max = x._2._1 //相差的天数
      val ent_id = x._2._2 // ent_id
      (ent_id, day_max)
    }).groupByKey.map(x => {
      val su = x._2.map(x => x.toDouble).sum
      val si = x._2.map(x => x.toDouble).size
      val avg = su / si
      val end_result = numberFormat.format(avg)
      //      (x._1, end_result, "ent_report_time")
      (x._1, avg.toString, "ent_report_time")
    })
    //      .take(10).foreach(println(_))
    (end_result.foreach(println))
    end_result
  }

  def Claminfo(sqlContext: HiveContext, sc: SparkContext) {

    //HBaseConf
    val conf = HbaseConf("labels:label_user_enterprise_vT")._1
    val conf_fs = HbaseConf("labels:label_user_enterprise_vT")._2
    val tableName = "labels:label_user_enterprise_vT"
    val columnFamily1 = "claiminfo"

    //    employer_liability_claims :雇主保理赔记录表    //已经报了案的
    //            意思是：
    //              也可以理解为出险表，可以这样想，我生病了去看病，要报销，这就可以是一个数据
    //    ods_policy_detail：保单表
    //    dim_product	企业产品信息表

    val employer_liability_claims: DataFrame = sqlContext.sql("select * from odsdb_prd.employer_liability_claims").cache

    val ods_policy_detail: DataFrame = sqlContext.sql("select * from odsdb_prd.ods_policy_detail").cache
    val dim_product = sqlContext.sql("select * from odsdb_prd.dim_product").filter("product_type_2='蓝领外包'").select("product_code").cache()
    val bro_dim: Broadcast[Array[String]] = sc.broadcast(dim_product.map(_.get(0).toString).collect)
    val ods_policy_insured_charged = sqlContext.sql("select * from odsdb_prd.ods_policy_insured_charged")
    val ods_policy_risk_period = sqlContext.sql("select * from odsdb_prd.ods_policy_risk_period").cache //ods_policy_risk_period:投保到报案
    val ods_policy_insured_detail = sqlContext.sql("select * from odsdb_prd.ods_policy_insured_detail")
    val d_work_level = sqlContext.sql("select * from odsdb_prd.d_work_level").cache
    val ods_policy_preserve_detail = sqlContext.sql("select * from odsdb_prd.ods_policy_preserve_detail")

    employer_liability_claims.show(100)
    //企业报案时效（小时）
    val ent_report_time_r: RDD[(String, String, String)] = ent_report_time(employer_liability_claims, ods_policy_detail).distinct()
    //    toHbase(ent_report_time_r, columnFamily1, "ent_report_time", conf_fs, tableName, conf)
  }

  def main(args: Array[String]): Unit = {
    val confs = new SparkConf()
      .setAppName("wuYu")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
      .set("spark.sql.broadcastTimeout", "36000")
      .setMaster("local[2]")
    val lines_source = Source.fromURL(getClass.getResource("/config_scala.properties")).getLines.toSeq
    val location_mysql_url: String = lines_source(2).toString.split("==")(1)
    val location_mysql_url_dwdb: String = lines_source(10).toString.split("==")(1)
    val prop: Properties = new Properties

    val sc = new SparkContext(confs)
    val sqlContext: HiveContext = new HiveContext(sc)
    Claminfo(sqlContext: HiveContext, sc)
    //    Insure(sqlContext: HiveContext, location_mysql_url: String, location_mysql_url_dwdb: String, prop: Properties)
    //    val bool: Boolean = is_not_date("510222196505068016")
    //    if(is_not_date("2018/11/2")) println(true) else println(false)

    sc.stop()
  }
}
