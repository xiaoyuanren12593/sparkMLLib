package bzn.job.label.channel_claiminfo

import java.text.NumberFormat
import java.util.Properties
import java.util.regex.Pattern

import com.alibaba.fastjson.JSONObject
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by MK on 2018/11/1.
  */
object ChaClaiminfoTest extends ChaClaiminfoUntilTest {

  def is_not_chinese(str: String)
  : Boolean
  = {
    val p = Pattern.compile("[\u4e00-\u9fa5]")
    val m = p.matcher(str)
    m.find()
  }

  /**
    * 渠道报案时效（小时）
    **/
  def ent_report_time(employer_liability_claims_r: DataFrame, ods_policy_detail_r: DataFrame,
                      get_hbase_key_name: collection.Map[String, String], sqlContext: HiveContext,
                      ods_ent_guzhu_salesman_channel: RDD[(String, String)],
                      en_before: collection.Map[String, String]): RDD[(String, String, String)] = {
    import sqlContext.implicits._

    //report_date:报案日期
    //risk_date:出险日期
    val numberFormat = NumberFormat.getInstance
    numberFormat.setMaximumFractionDigits(2)
    val employer_liability_claims = employer_liability_claims_r
      .filter("length(report_date)>3 and length(risk_date)>3")
      .select("policy_no", "report_date", "risk_date")
      .map(x => x)
      .filter(x => {
        val date3 = x.getString(1)
        val date4 = x.getString(2)
        if (!is_not_chinese(date3) && !is_not_chinese(date4)) true else false
      })
      .map(x => {
        val date3 = x.getString(1)
        val date4 = x.getString(2)
        val day_mix = xg(date3, date4)
        //日期相同的话，日期相减是0，因此如果是0的话，那么我们返回1天来进行计算，
        val dm = if (day_mix == 0) 1 else day_mix
        //保单号，出险与保险相差的天数(报>=出)
        (x.getString(0), dm)
      })
    val ods_policy_detail = ods_policy_detail_r
      .select("ent_id", "policy_code")
      .map(x => (x.getString(1), x.getString(0)))
    //根据保单ID做一个Join，找到我对应的在保单ID中的数据(企业ent_id在保单表中)
    //ent_id，小时(存到了hbase中的ent_report_time)
    val end_result = employer_liability_claims
      .join(ods_policy_detail)
      .map(x => {
        val day_max = x._2._1 //相差的天数
        val ent_id = x._2._2 // ent_id
        (ent_id, (day_max, 1))
      })
      .reduceByKey((x1, x2) => {
        val su = x1._1 + x2._1
        val si = x1._2 + x2._2
        (su, si)
      })
      .map(x => {
        val su = x._2._1
        val si = x._2._2
        (s"${get_hbase_key_name.getOrElse(x._1, "null")}", su.toString, si.toString)
      })
      .toDF("ent_name", "su", "si")

    //根据企业名称进行join
    val end = end_result
      .join(ods_ent_guzhu_salesman_channel.toDF("channel_name", "ent_name"), "ent_name")
      .map(x => {
        val channel_name = x.getAs[String]("channel_name")
        val su = x.getAs[String]("su").toInt
        val si = x.getAs[String]("si").toInt
        (channel_name, (su, si))
      })
      .reduceByKey((x1, x2) => {
        //根据渠道名称进行分组，并将各个企业的男生人数，女生人数，总人数求和
        val su = x1._1 + x2._1
        val si = x1._2 + x2._2
        (su, si)
      })
      .map(x => {
        val channel_name_id = en_before.getOrElse(x._1, "null")
        val su = x._2._1.toDouble
        val si = x._2._2.toDouble
        val avg = su / si
        (channel_name_id, avg.toString, "ent_report_time")
      })

    end
  }

  /**
    * 渠道平均申请理赔周期(天)
    **/
  def avg_aging_claim(employer_liability_claims: DataFrame, ods_policy_detail: DataFrame,
                      get_hbase_key_name: collection.Map[String, String], sqlContext: HiveContext,
                      ods_ent_guzhu_salesman_channel: RDD[(String, String)],
                      en_before: collection.Map[String, String]): RDD[(String, String, String)] = {
    import sqlContext.implicits._

    //先计算我出险到我材料补充齐全,经过了多好天.
    val tep_one = employer_liability_claims
      .filter("LENGTH(paper_finish_date)>0 and  length(risk_date)>3")
      .select("policy_no", "paper_finish_date", "risk_date")
      .map(x => x)
      .filter(x => {
        val paper_finish_date = x.getString(1)
        val risk_date = x.getString(2)
        if (!is_not_chinese(paper_finish_date) && !is_not_chinese(risk_date)) true else false
      })
      .map(x => {
        val paper_finish_date = x.getString(1)
        val risk_date = x.getString(2)
        val days = xg(paper_finish_date, risk_date)
        //police_no | 补齐的天数
        (x.getString(0), days)
      })
    val tep_two = ods_policy_detail
      .filter("LENGTH(ent_id)>0")
      .select("policy_code", "ent_id")
      .map(x => (x.getString(0), x.getString(1)))
    val end_result = tep_one
      .join(tep_two)
      //ent_id | 每个人补齐的天数 |1
      .map(x => (x._2._2, (x._2._1.toDouble, 1)))
      .reduceByKey((x1, x2) => {
        val days = x1._1 + x2._1
        val count = x1._2 + x2._2
        (days, count)
      })
      .map(x => (s"${get_hbase_key_name.getOrElse(x._1, "null")}", x._2._1.toString, x._2._2.toString))
      .toDF("ent_name", "days", "count")

    //根据企业名称进行join
    val end = end_result
      .join(ods_ent_guzhu_salesman_channel.toDF("channel_name", "ent_name"), "ent_name")
      .map(x => {
        val channel_name = x.getAs[String]("channel_name")
        val days = x.getAs[String]("days").toDouble
        val count = x.getAs[String]("count").toDouble
        (channel_name, (days, count))
      })
      .reduceByKey((x1, x2) => {
        //根据渠道名称进行分组
        val days = x1._1 + x2._1
        val count = x1._2 + x2._2
        (days, count)
      })
      .map(x => {
        val channel_name_id = en_before.getOrElse(x._1, "null")
        val days = x._2._1
        val count = x._2._2
        val avg = days / count
        (channel_name_id, avg.toString, "avg_aging_claim")
      })

    end
  }

  def Claminfo(users_rdd: RDD[String], channel_ent_name: Array[String], ods_ent_guzhu_salesman_channel_rdd: RDD[(String, String)],
               sql_context: HiveContext, get_hbase_key_name: collection.Map[String, String],
               en_before: collection.Map[String, String]): Unit = {
    //ods_policy_risk_period:投保到报案
    val ods_policy_risk_period = sql_context.sql("select * from odsdb_prd.ods_policy_risk_period").cache
    val employer_liability_claims: DataFrame = sql_context.sql("select * from odsdb_prd.employer_liability_claims").cache
    val ods_policy_detail: DataFrame = sql_context.sql("select * from odsdb_prd.ods_policy_detail").cache
    val ods_policy_insured_detail = sql_context.sql("select * from odsdb_prd.ods_policy_insured_detail")

    //HBaseConf
    val conf: Configuration = HbaseConf("labels:label_channel_vT")._1
    val conf_fs: Configuration = HbaseConf("labels:label_channel_vT")._2
    val tableName: String = "labels:label_channel_vT"
    val columnFamily: String = "claiminfo"

    //过滤处字符串中存在上述渠道企业的数据
    val before: RDD[(String, String)] = users_rdd.map(x => (x.split("mk6")(0), x.split("mk6")(1)))

    //过滤出标签中的渠道，渠道风险等级 ，里面有toHbase
    val channel_before: RDD[(String, String)] = before.filter(x => channel_ent_name.contains(x._1))
    channel_before.take(10).foreach(println)
//    ent_work_risk_r_to_hbase(conf, conf_fs, tableName, columnFamily, channel_before, before, sql_context,
//      ods_ent_guzhu_salesman_channel_rdd, en_before)

    //渠道增减行为(人数):在同一个年单中超过2次减员的人的个数
    val ent_employee_increase_r = channel_add(before, ods_ent_guzhu_salesman_channel_rdd, sql_context,
      en_before, "ent_employee_increase").filter(_._1.length > 5)
//    saveToHbase(ent_employee_increase_r, columnFamily, "ent_employee_increase", conf_fs, tableName, conf)
    ent_employee_increase_r.take(10).foreach(println)
    //渠道出险概率,每百人月均出险概率
    val ent_monthly_risk_r = ent_monthly_risk(employer_liability_claims, ods_policy_detail, ods_policy_insured_detail,
      get_hbase_key_name, sql_context, ods_ent_guzhu_salesman_channel_rdd, en_before).filter(_._1.length > 5)
//    saveToHbase(ent_monthly_risk_r, columnFamily, "ent_monthly_risk", conf_fs, tableName, conf)
    ent_monthly_risk_r.take(10).foreach(println)
    //渠道材料完整度
    val ent_material_integrity_r = ent_material_integrity(employer_liability_claims, ods_policy_detail, get_hbase_key_name,
      sql_context, ods_ent_guzhu_salesman_channel_rdd, en_before).filter(_._1.length > 5)
//    saveToHbase(ent_material_integrity_r, columnFamily, "ent_material_integrity", conf_fs, tableName, conf)
    ent_material_integrity_r.take(10).foreach(println)
    //渠道报案时效（小时）
    val ent_report_time_r = ent_report_time(employer_liability_claims, ods_policy_detail, get_hbase_key_name,
      sql_context, ods_ent_guzhu_salesman_channel_rdd, en_before).filter(_._1.length > 5)
//    saveToHbase(ent_report_time_r, columnFamily, "ent_report_time", conf_fs, tableName, conf)
    ent_report_time_r.take(10).foreach(println)
    //渠道平均申请理赔周期(天)
    val avg_aging_claim_r = avg_aging_claim(employer_liability_claims, ods_policy_detail, get_hbase_key_name,
      sql_context, ods_ent_guzhu_salesman_channel_rdd, en_before).filter(_._1.length > 5)
//    saveToHbase(avg_aging_claim_r, columnFamily, "avg_aging_claim", conf_fs, tableName, conf)
      avg_aging_claim_r.take(10).foreach(println)
    //渠道平均赔付时效
    val avg_aging_cps_r = avg_aging_cps(employer_liability_claims, ods_policy_detail, get_hbase_key_name,
      sql_context, ods_ent_guzhu_salesman_channel_rdd, en_before).filter(_._1.length > 5)
//    saveToHbase(avg_aging_cps_r, columnFamily, "avg_aging_cps", conf_fs, tableName, conf)
    avg_aging_cps_r.take(10).foreach(println)
    //渠道报案件数
    val report_num_r = channel_add(before, ods_ent_guzhu_salesman_channel_rdd, sql_context,
      en_before, "report_num").filter(_._1.length > 5)
//    saveToHbase(report_num_r, columnFamily, "report_num", conf_fs, tableName, conf)
    report_num_r.take(10).foreach(println)
    //渠道理赔件数
    val claim_num_r = channel_add(before, ods_ent_guzhu_salesman_channel_rdd, sql_context,
      en_before, "claim_num").filter(_._1.length > 5)
//    saveToHbase(claim_num_r, columnFamily, "claim_num", conf_fs, tableName, conf)
    claim_num_r.take(10).foreach(println)
    //渠道死亡案件统计
    val death_num_r = channel_add(before, ods_ent_guzhu_salesman_channel_rdd, sql_context,
      en_before, "death_num").filter(_._1.length > 5)
//    saveToHbase(death_num_r, columnFamily, "death_num", conf_fs, tableName, conf)
    death_num_r.take(10).foreach(println)
    //渠道伤残案件数
    val disability_num_r = channel_add(before, ods_ent_guzhu_salesman_channel_rdd, sql_context,
      en_before, "disability_num").filter(_._1.length > 5)
//    saveToHbase(disability_num_r, columnFamily, "disability_num", conf_fs, tableName, conf)
    disability_num_r.take(10).foreach(println)
    //渠道工作期间案件数
    val worktime_num_r = channel_add(before, ods_ent_guzhu_salesman_channel_rdd, sql_context,
      en_before, "worktime_num").filter(_._1.length > 5)
//    saveToHbase(worktime_num_r, columnFamily, "worktime_num", conf_fs, tableName, conf)
    worktime_num_r.take(10).foreach(println)
    //渠道非工作期间案件数
    val nonworktime_num_r = channel_add(before, ods_ent_guzhu_salesman_channel_rdd, sql_context,
      en_before, "nonworktime_num").filter(_._1.length > 5)
//    saveToHbase(nonworktime_num_r, columnFamily, "nonworktime_num", conf_fs, tableName, conf)
    nonworktime_num_r.take(10).foreach(println)
    //渠道预估总赔付金额
    val pre_all_compensation_r = channel_add(before, ods_ent_guzhu_salesman_channel_rdd, sql_context,
      en_before, "pre_all_compensation").filter(_._1.length > 5)
//    saveToHbase(pre_all_compensation_r, columnFamily, "pre_all_compensation", conf_fs, tableName, conf)
    pre_all_compensation_r.take(10).foreach(println)
    //渠道死亡预估配额
    val pre_death_compensation_r = channel_add(before, ods_ent_guzhu_salesman_channel_rdd, sql_context,
      en_before, "pre_death_compensation").filter(_._1.length > 5)
//    saveToHbase(pre_death_compensation_r, columnFamily, "pre_death_compensation", conf_fs, tableName, conf)
    pre_all_compensation_r.take(10).foreach(println)
    //渠道伤残预估配额
    val pre_dis_compensation_r = channel_add(before, ods_ent_guzhu_salesman_channel_rdd, sql_context,
      en_before, "pre_dis_compensation").filter(_._1.length > 5)
//    saveToHbase(pre_dis_compensation_r, columnFamily, "pre_dis_compensation", conf_fs, tableName, conf)
    pre_dis_compensation_r.take(10).foreach(println)
    //渠道工作期间预估赔付
    val pre_wt_compensation_r = channel_add(before, ods_ent_guzhu_salesman_channel_rdd, sql_context,
      en_before, "pre_wt_compensation").filter(_._1.length > 5)
//    saveToHbase(pre_wt_compensation_r, columnFamily, "pre_wt_compensation", conf_fs, tableName, conf)
    pre_wt_compensation_r.take(10).foreach(println)
    //渠道非工作期间预估赔付
    val pre_nwt_compensation_r = channel_add(before, ods_ent_guzhu_salesman_channel_rdd, sql_context,
      en_before, "pre_nwt_compensation").filter(_._1.length > 5)
//    saveToHbase(pre_nwt_compensation_r, columnFamily, "pre_nwt_compensation", conf_fs, tableName, conf)
    pre_nwt_compensation_r.take(10).foreach(println)
    //渠道实际已赔付金额
    val all_compensation_r = channel_add(before, ods_ent_guzhu_salesman_channel_rdd, sql_context,
      en_before, "all_compensation").filter(_._1.length > 5)
//    saveToHbase(all_compensation_r, columnFamily, "all_compensation", conf_fs, tableName, conf)

    //渠道超时赔付案件数
    val overtime_compensation_r = channel_add(before, ods_ent_guzhu_salesman_channel_rdd, sql_context,
      en_before, "overtime_compensation").filter(_._1.length > 5)
//    saveToHbase(overtime_compensation_r, columnFamily, "overtime_compensation", conf_fs, tableName, conf)
    overtime_compensation_r.take(10).foreach(println)
    //渠道已赚保费
    val charged_premium_r = channel_add(before, ods_ent_guzhu_salesman_channel_rdd, sql_context,
      en_before, "charged_premium").filter(_._1.length > 5)
//    saveToHbase(charged_premium_r, columnFamily, "charged_premium", conf_fs, tableName, conf)
    charged_premium_r.take(10).foreach(println)
    //渠道拒赔次数
    val ent_rejected_count_r = channel_add(before, ods_ent_guzhu_salesman_channel_rdd, sql_context,
      en_before, "ent_rejected_count").filter(_._1.length > 5)
//    saveToHbase(ent_rejected_count_r, columnFamily, "ent_rejected_count", conf_fs, tableName, conf)
    ent_rejected_count_r.take(10).foreach(println)
    //渠道撤案次数
    val ent_withdrawn_count_r = channel_add(before, ods_ent_guzhu_salesman_channel_rdd, sql_context,
      en_before, "ent_withdrawn_count").filter(_._1.length > 5)
//    saveToHbase(ent_withdrawn_count_r, columnFamily, "ent_withdrawn_count", conf_fs, tableName, conf)
    ent_withdrawn_count_r.take(10).foreach(println)
    //渠道平均出险周期
    val avg_aging_risk_r = avg_aging_risk(ods_policy_detail, ods_policy_risk_period, get_hbase_key_name,
      sql_context, ods_ent_guzhu_salesman_channel_rdd, en_before).filter(_._1.length > 5)
//    saveToHbase(avg_aging_risk_r, columnFamily, "avg_aging_risk", conf_fs, tableName, conf)
    avg_aging_risk_r.take(10).foreach(println)
    //渠道极短周期个数（出险周期小于3的案件数）
    val mix_period_count_r = channel_add(before, ods_ent_guzhu_salesman_channel_rdd, sql_context,
      en_before, "mix_period_count").filter(_._1.length > 5)
//    saveToHbase(mix_period_count_r, columnFamily, "mix_period_count", conf_fs, tableName, conf)
    mix_period_count_r.take(10).foreach(println)
    //渠道极短周期百分比
    val mix_period_rate_r = mix_period_rate(ods_policy_detail, ods_policy_risk_period, get_hbase_key_name, sql_context,
      ods_ent_guzhu_salesman_channel_rdd, en_before).filter(_._1.length > 5)
//    saveToHbase(mix_period_rate_r, columnFamily, "mix_period_rate", conf_fs, tableName, conf)
    mix_period_rate_r.take(10).foreach(println)
    //渠道重大案件率
    val largecase_rate_r = largecase_rate(ods_policy_detail, employer_liability_claims, get_hbase_key_name, sql_context, ods_ent_guzhu_salesman_channel_rdd, en_before).filter(_._1.length > 5)
//    saveToHbase(largecase_rate_r, columnFamily, "largecase_rate", conf_fs, tableName, conf)
    largecase_rate_r.take(10).foreach(println)
  }

  def main(args: Array[String]): Unit = {
    //得到标签数据

    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf_spark = new SparkConf().setAppName("Cha_claiminfo")
    conf_spark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf_spark.registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
    conf_spark.set("spark.sql.broadcastTimeout", "36000")
          .setMaster("local[4]")

    val sc: SparkContext = new SparkContext(conf_spark)
    val sqlContext: HiveContext = new HiveContext(sc)


    //读取渠道表
    val lines_source = Source.fromURL(getClass.getResource("/config_scala.properties")).getLines.toSeq
    val location_mysql_url: String = lines_source(2).toString.split("==")(1)
    val prop: Properties = new Properties

    //读取渠道表（不分组）
    val ods_ent_guzhu_salesman_channel: RDD[(String, String)] = sqlContext.read.jdbc(location_mysql_url, "ods_ent_guzhu_salesman", prop).map(x => {
      val ent_name = x.getAs[String]("ent_name").trim
      val channel_name = x.getAs[String]("channel_name").trim
      val new_channel_name = if (channel_name == "直客") ent_name else channel_name
      (new_channel_name, ent_name)
    })

    //以渠道分组后的渠道表
    val ods_ent_guzhu_salesman_channel_only_channel = ods_ent_guzhu_salesman_channel.groupByKey.map(x => (x._1, x._2.mkString("mk6"))).persist(StorageLevel.MEMORY_ONLY)

    //得到渠道企业
    val channel_ent_name: Array[String] = ods_ent_guzhu_salesman_channel_only_channel.map(_._1).collect

    //得到标签数据
    val usersRDD: RDD[String] = getHbase_value(sc).map(tuple => tuple._2).map(result => {
      val ent_name = Bytes.toString(result.getValue("baseinfo".getBytes, "ent_name".getBytes))
      (ent_name, result.raw)
    }).mapPartitions(rdd => {
      val json: JSONObject = new JSONObject
      rdd.map(f => KeyValueToString(f._2, json, f._1)) //.flatMap(_.split(";"))
    }).cache

    //得到标签数据企业ID，与企业名称
    val get_hbase_key_name: collection.Map[String, String] = getHbase_value(sc).map(tuple => tuple._2).map(result => {
      val key = Bytes.toString(result.getRow)
      val ent_name = Bytes.toString(result.getValue("baseinfo".getBytes, "ent_name".getBytes))
      (key, ent_name)
    }).collectAsMap

    //渠道名称和渠道ID
    val en_before = sqlContext.read.jdbc(location_mysql_url, "ods_ent_guzhu_salesman", prop).map(x => {
      val ent_name = x.getAs[String]("ent_name").trim
      val channel_name = x.getAs[String]("channel_name").trim
      val new_channel_name = if (channel_name == "直客") ent_name else channel_name
      val channel_id = x.getAs[String]("channel_id")
      (new_channel_name, channel_id)
    }).filter(x => if (x._1.length > 5 && x._2 != "null") true else false).collectAsMap()

    Claminfo(usersRDD, channel_ent_name, ods_ent_guzhu_salesman_channel, sqlContext, get_hbase_key_name, en_before)
    sc.stop()
  }
}
