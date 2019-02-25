package bzn.job.label.ent_claiminfo

import java.text.{NumberFormat, SimpleDateFormat}
import java.util.Properties
import java.util.regex.Pattern

import bzn.job.common.Until
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source


object EntClaiminfoTest extends ClaiminfoUntilTest with Until {

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
      case e: Throwable => convertSuccess = false
    };
    convertSuccess
  }

  //理由同上类 : 27
  //企业报案时效（小时）
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
      val date4 = x.getString(2)
      if (!is_not_chinese(date3) && !is_not_chinese(date4)) {
        if(is_not_date(date3) && is_not_date(date4)) {
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
      val day_mix = xg(date3, date4)
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
    //      .show()
    end_result
  }

  //平均申请理赔周期(天)
  def avg_aging_claim(employer_liability_claims: DataFrame, ods_policy_detail: DataFrame): RDD[(String, String, String)] = {
    //  if_resubmit_continue:是否需要补充材料
    //  paper_finish_date:材料补充完成日期（比如我生病了: 会出事一些病例之类的，这些都是资料）
    //  risk_date:出险日期 (我报案的时候，要出示我什么时候摔断了腿了---摔断腿这个险情就是出险日期)

    val numberFormat = NumberFormat.getInstance
    numberFormat.setMaximumFractionDigits(2)


    //先计算我出险到我材料补充齐全,经过了多好天.
    val tep_one = employer_liability_claims.filter("LENGTH(paper_finish_date)>0 and  length(risk_date)>3").select("policy_no", "paper_finish_date", "risk_date")
      //      .show()
      //    |         policy_no|paper_finish_date|risk_date|
      //    |900000046998381950|         2018/1/8| 2017/8/21|
      .map(x => x).filter(x => {
      val paper_finish_date = x.getString(1)
      val risk_date = x.getString(2)
      if (!is_not_chinese(paper_finish_date) && !is_not_chinese(risk_date)) true else false
    }).map(x => {
      val paper_finish_date = x.getString(1)
      val risk_date = x.getString(2)
      val days = xg(paper_finish_date, risk_date)
      //police_no | 补齐的天数
      (x.getString(0), days)
    })
    val tep_two = ods_policy_detail.filter("LENGTH(ent_id)>0").select("policy_code", "ent_id")
      //    val tep_two = sqlContext.sql("select pd.policy_code,pd.ent_id from odsdb_prd.ods_policy_detail pd").filter("LENGTH(ent_id)>0")
      .map(x => {
      //policy_code | ent_id
      (x.getString(0), x.getString(1))
    })
    //根据保单号，来跟保单表进行join，相关联
    //再根据企业ID进行分组，找出企业ID对应的都有哪些人（每个人的补齐天数） 求个平均数(使用reduceByKey进行计算)
    //将其存入到Hbase中的avg_aging_claim字段
    val end = tep_one.join(tep_two).map(x => {
      //ent_id | 每个人补齐的天数 |1
      (x._2._2, (x._2._1.toDouble, 1))
    }).reduceByKey((x1, x2) => {
      val days = x1._1 + x2._1
      val count = x1._2 + x2._2
      (days, count)
    }).map(x => {
      val s = x._2._1 / x._2._2
      // ent_id |  平均数
      // (x._1, numberFormat.format(s), "avg_aging_claim")
      (x._1, s.toString, "avg_aging_claim")
    })
    end
  }

  def Claminfo(ods_ent_guzhu_salesman_channel: RDD[(String, String)],sqlContext: HiveContext, sc: SparkContext) {

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
    //    val employer_liability_claims_tep_one: DataFrame = sqlContext.sql("select * from odsdb_prd.employer_liability_claims").cache

    //    val employer_liability_claims_fields = employer_liability_claims_tep_one.schema.map(x => (x.name, x.dataType))
    //    val value = employer_liability_claims_tep_one.map(x => {
    //      val paper_finish_date = x.getAs[String]("paper_finish_date")
    //      val risk_date = x.getAs[String]("risk_date")
    //      (is_not_chinese(paper_finish_date), is_not_chinese(risk_date), x)
    //    }).filter(x => if (!x._1 || !x._2) true else false).map(x => x._3) //将字段中的中文过滤掉
    //
    //    val schema = StructType(employer_liability_claims_fields.map(field => StructField(field._1, field._2, nullable = true)))
    //    val employer_liability_claims = sqlContext.createDataFrame(value, schema) //.show()



    val ods_policy_detail: DataFrame = sqlContext.sql("select * from odsdb_prd.ods_policy_detail")
      .cache

    val dim_product = sqlContext.sql("select * from odsdb_prd.dim_product").filter("product_type_2='蓝领外包' and dim_1 in ('外包雇主', '骑士保', '大货车')").select("product_code").cache()
    val bro_dim: Broadcast[Array[String]] = sc.broadcast(dim_product.map(_.get(0).toString).collect)
    val ods_policy_insured_charged = sqlContext.sql("select * from odsdb_prd.ods_policy_insured_charged")
    val ods_policy_risk_period = sqlContext.sql("select * from odsdb_prd.ods_policy_risk_period").cache //ods_policy_risk_period:投保到报案
    val ods_policy_insured_detail = sqlContext.sql("select * from odsdb_prd.ods_policy_insured_detail")
    val d_work_level = sqlContext.sql("select * from odsdb_prd.d_work_level").cache
    val ods_policy_preserve_detail = sqlContext.sql("select * from odsdb_prd.ods_policy_preserve_detail")


    //工种风险等级：对应的有多少人
    val ent_work_risk_r: RDD[(String, String, String)] = ent_work_risk(ods_policy_insured_detail, ods_policy_detail, d_work_level).cache()
//    ent_work_risk_r_To_Hbase(ent_work_risk_r, columnFamily1, conf_fs, tableName, conf)
    ent_work_risk_r.take(10).foreach(println)
    //员工增减行为(人数):在同一个年单中超过2次减员的人的个数
    val ent_employee_increase_r = ent_employee_increase(ods_policy_preserve_detail, ods_policy_detail).distinct
    ent_employee_increase_r.take(10).foreach(println)
    //    toHbase(ent_employee_increase_r, columnFamily1, "ent_employee_increase", conf_fs, tableName, conf)

    //c,每百人月均出险概率（逻辑改为:  每百人出险人数=总出险概率*100）
    val ent_monthly_risk_r = ent_monthly_risk(employer_liability_claims, ods_policy_detail, ods_policy_insured_detail).distinct
    ent_monthly_risk_r.take(10).foreach(println)
//    toHbase(ent_monthly_risk_r, columnFamily1, "ent_monthly_risk", conf_fs, tableName, conf)

    //材料完整度
    val ent_material_integrity_r = ent_material_integrity(employer_liability_claims, ods_policy_detail).distinct()
    ent_material_integrity_r.take(10).foreach(println)
    //    toHbase(ent_material_integrity_r, columnFamily1, "ent_material_integrity", conf_fs, tableName, conf)

    //企业报案时效（小时）
    val ent_report_time_r = ent_report_time(employer_liability_claims, ods_policy_detail).distinct()
    ent_report_time_r.take(10).foreach(println)
//    toHbase(ent_report_time_r, columnFamily1, "ent_report_time", conf_fs, tableName, conf)


    //平均申请理赔周期(天)
    val avg_aging_claim_r = avg_aging_claim(employer_liability_claims, ods_policy_detail).distinct()
    avg_aging_claim_r.take(10).foreach(println)
//    toHbase(avg_aging_claim_r, columnFamily1, "avg_aging_claim", conf_fs, tableName, conf)


    //平均赔付时效
    val avg_aging_cps_r = avg_aging_cps(employer_liability_claims, ods_policy_detail).distinct()
    avg_aging_cps_r.take(10).foreach(println)
    //    toHbase(avg_aging_cps_r, columnFamily1, "avg_aging_cps", conf_fs, tableName, conf)


    //企业报案件数
    val report_num_r = report_num(employer_liability_claims, ods_policy_detail).distinct()
    report_num_r.take(10).foreach(println)
    //    toHbase(report_num_r, columnFamily1, "report_num", conf_fs, tableName, conf)


    //企业理赔件数
    val claim_num_r = claim_num(employer_liability_claims, ods_policy_detail).distinct()
    claim_num_r.take(10).foreach(println)
//    toHbase(claim_num_r, columnFamily1, "claim_num", conf_fs, tableName, conf)


    //死亡案件统计
    val death_num_r = death_num(employer_liability_claims, ods_policy_detail).distinct()
    death_num_r.take(10).foreach(println)
    //    toHbase(death_num_r, columnFamily1, "death_num", conf_fs, tableName, conf)


    //伤残案件数
    val disability_num_r = disability_num(employer_liability_claims, ods_policy_detail).distinct()
    disability_num_r.take(10).foreach(println)
//    toHbase(disability_num_r, columnFamily1, "disability_num", conf_fs, tableName, conf)


    //工作期间案件数
    val worktime_num_r = worktime_num(employer_liability_claims, ods_policy_detail).distinct()
    worktime_num_r.take(10).foreach(println)
//    toHbase(worktime_num_r, columnFamily1, "worktime_num", conf_fs, tableName, conf)


    //非工作期间案件数
    val nonworktime_num_r = nonworktime_num(employer_liability_claims, ods_policy_detail).distinct()
    nonworktime_num_r.take(10).foreach(println)
//    toHbase(nonworktime_num_r, columnFamily1, "nonworktime_num", conf_fs, tableName, conf)


    //预估总赔付金额
    val pre_all_compensation_r = pre_all_compensation(ods_ent_guzhu_salesman_channel,sqlContext,bro_dim,employer_liability_claims, ods_policy_detail).distinct()
    pre_all_compensation_r.take(10).foreach(println)
//    toHbase(pre_all_compensation_r, columnFamily1, "pre_all_compensation", conf_fs, tableName, conf)


    //死亡预估配额
    val pre_death_compensation_r = pre_death_compensation(employer_liability_claims, ods_policy_detail).distinct()
    pre_death_compensation_r.take(10).foreach(println)
//    toHbase(pre_death_compensation_r, columnFamily1, "pre_death_compensation", conf_fs, tableName, conf)


    //伤残预估配额
    val pre_dis_compensation_r = pre_dis_compensation(employer_liability_claims, ods_policy_detail).distinct()
    pre_dis_compensation_r.take(10).foreach(println)
    //    toHbase(pre_dis_compensation_r, columnFamily1, "pre_dis_compensation", conf_fs, tableName, conf)


    //工作期间预估赔付
    val pre_wt_compensation_r = pre_wt_compensation(employer_liability_claims, ods_policy_detail).distinct()
    pre_wt_compensation_r.take(10).foreach(println)
//    toHbase(pre_wt_compensation_r, columnFamily1, "pre_wt_compensation", conf_fs, tableName, conf)


    //非工作期间预估赔付
    val pre_nwt_compensation_r = pre_nwt_compensation(employer_liability_claims, ods_policy_detail).distinct()
    pre_nwt_compensation_r.take(10).foreach(println)
    //    toHbase(pre_nwt_compensation_r, columnFamily1, "pre_nwt_compensation", conf_fs, tableName, conf)


    //实际已赔付金额
    val all_compensation_r = all_compensation(employer_liability_claims, ods_policy_detail).distinct()
    all_compensation_r.take(10).foreach(println)
//    toHbase(all_compensation_r, columnFamily1, "all_compensation", conf_fs, tableName, conf)


    //超时赔付案件数
    val overtime_compensation_r = overtime_compensation(employer_liability_claims, ods_policy_detail).distinct()
    overtime_compensation_r.take(10).foreach(println)
//    toHbase(overtime_compensation_r, columnFamily1, "overtime_compensation", conf_fs, tableName, conf)


    //已赚保费
    val lines_source = Source.fromURL(getClass.getResource("/config_scala.properties")).getLines.toSeq
    val location_mysql_url: String = lines_source(2).toString.split("==")(1)
    val prop: Properties = new Properties

    val charged_premium_r = charged_premium_new(sqlContext: HiveContext, location_mysql_url: String, prop: Properties, ods_policy_detail: DataFrame).distinct()
    charged_premium_r .take(10).foreach(println)

//    toHbase(charged_premium_r, columnFamily1, "charged_premium", conf_fs, tableName, conf)



    //企业拒赔次数
    val ent_rejected_count_r = ent_rejected_count(employer_liability_claims, ods_policy_detail).distinct()
    ent_rejected_count_r .take(10).foreach(println)
    //    toHbase(ent_rejected_count_r, columnFamily1, "ent_rejected_count", conf_fs, tableName, conf)


    //企业撤案次数
    val ent_withdrawn_count_r = ent_withdrawn_count(employer_liability_claims, ods_policy_detail).distinct()
    ent_withdrawn_count_r .take(10).foreach(println)
    //    toHbase(ent_withdrawn_count_r, columnFamily1, "ent_withdrawn_count", conf_fs, tableName, conf)


    //平均出险周期
    val avg_aging_risk_r = avg_aging_risk(ods_policy_detail, ods_policy_risk_period).distinct()
    avg_aging_risk_r.take(10).foreach(println)
    //    toHbase(avg_aging_risk_r, columnFamily1, "avg_aging_risk", conf_fs, tableName, conf)


    //极短周期特征（出险周期小于3的案件数）
    val mix_period_count_r = mix_period_count(ods_policy_detail, ods_policy_risk_period).distinct()
    mix_period_count_r.take(10).foreach(println)
    //    toHbase(mix_period_count_r, columnFamily1, "mix_period_count", conf_fs, tableName, conf)


    //极短周期百分比(只取极短特征个数大于1的企业)
    val mix_period_rate_r = mix_period_rate(ods_policy_detail, ods_policy_risk_period).distinct
    mix_period_rate_r .take(10).foreach(println)
    //    toHbase(mix_period_rate_r, columnFamily1, "mix_period_rate", conf_fs, tableName, conf)


    //重大案件率
    val largecase_rate_r = largecase_rate(ods_policy_detail, employer_liability_claims).distinct()
    largecase_rate_r.take(10).foreach(println)
    //    toHbase(largecase_rate_r, columnFamily1, "largecase_rate", conf_fs, tableName, conf)
  }

  def main(args: Array[String]): Unit = {
    val conf_s = new SparkConf().setAppName("wuYu")
    val lines_source = Source.fromURL(getClass.getResource("/config_scala.properties")).getLines.toSeq
    val location_mysql_url: String = lines_source(2).toString.split("==")(1)
    conf_s.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //使用spark的序列化
    conf_s.registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
    conf_s.set("spark.sql.broadcastTimeout", "36000") //等待时长
              .setMaster("local[2]")
    val sc = new SparkContext(conf_s)
    val prop: Properties = new Properties
    val sqlContext: HiveContext = new HiveContext(sc)
    val ods_ent_guzhu_salesman: Array[String] = sqlContext.read.jdbc(location_mysql_url, "ods_ent_guzhu_salesman", prop).map(x => x.getAs[String]("ent_name").trim).distinct.collect
    val ods_ent_guzhu_salesman_channel: RDD[(String, String)] = sqlContext.read.jdbc(location_mysql_url, "ods_ent_guzhu_salesman", prop).map(x => {
      val ent_name = x.getAs[String]("ent_name").trim
      val channel_name = x.getAs[String]("channel_name")
      val new_channel_name = if (channel_name == "直客") ent_name else channel_name
      (ent_name, new_channel_name)
    }).filter(x => if (ods_ent_guzhu_salesman.contains(x._1)) true else false).persist(StorageLevel.MEMORY_ONLY)

    Claminfo(ods_ent_guzhu_salesman_channel,sqlContext: HiveContext, sc)
    sc.stop()
  }
}
