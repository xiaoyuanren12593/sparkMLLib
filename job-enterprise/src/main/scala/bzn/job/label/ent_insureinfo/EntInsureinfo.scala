package bzn.job.label.ent_insureinfo

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import bzn.job.common.Until
import bzn.job.until.EnterpriseUntil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.io.Source

object EntInsureinfo extends InsureinfoUntil with EnterpriseUntil  {
  //21
  //首次投保至今月数
  def ent_fist_plc_month(ods_policy_detail: DataFrame): RDD[(String, String, String)] = {

    val nowData: String = getNowTime()
    val after = currentTimeL(nowData) //得到当前时间的时间戳

    val end: RDD[(String, String, String)] = ods_policy_detail.filter("length(ent_id)>0 and length(policy_create_time)>0")
      .where("policy_status in('0','1','7','9','10')")
      .select("ent_id", "policy_create_time").map(x => {

      val format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
      val ent_id = x.getString(0)
      val data_time = x.getString(1)

      val insured_create_time_curr_one: DateTime = DateTime.parse(data_time, format)
      val insured_create_time_curr_Long = insured_create_time_curr_one.getMillis

      (ent_id, insured_create_time_curr_Long)
    }).reduceByKey((x1, x2) => {
      val res = if (x1 < x2) {
        x1
      } else {
        x2
      }
      res
    }).map(x => {
      val day = (after - x._2) / (24 * 60 * 60 * 1000)
      val month = day / 30
      val result = if (month == 0) 1 else month.toInt
      (x._1, result + "", "ent_fist_plc_month")
    })
    end
  }

  def Insure(sqlContext: HiveContext, location_mysql_url: String, location_mysql_url_dwdb: String, prop: Properties) {
    //    employer_liability_claims :雇主保理赔记录表
    //            意思是：
    //              也可以理解为出险表，可以这样想，我生病了去看病，要报销，这就可以是一个数据
    //    ods_policy_detail：保单表
    //    dim_product	企业产品信息表
    val ods_policy_detail: DataFrame = sqlContext.sql("select * from odsdb_prd.ods_policy_detail").cache()
    //被保人明细表
    val ods_policy_insured_detail = sqlContext.sql("select * from odsdb_prd.ods_policy_insured_detail")

    //保全明细表
    val ods_policy_preserve_detail = sqlContext.sql("select * from odsdb_prd.ods_policy_preserve_detail")
    val ent_summary_month_1 = sqlContext.sql("select * from odsdb_prd.ent_summary_month_1").cache()
    val ent_summary_month = sqlContext.sql("select * from odsdb_prd.ent_summary_month")
    val d_work_level = sqlContext.sql("select * from odsdb_prd.d_work_level").cache()
    val employer_liability_claims = sqlContext.sql("select * from odsdb_prd.employer_liability_claims").cache()
    val ent_sum_level = sqlContext.sql("select * from odsdb_prd.ent_sum_level").cache()

    //HBaseConf
    val conf = HbaseConf("labels:label_user_enterprise_vT")._1
    val conf_fs = HbaseConf("labels:label_user_enterprise_vT")._2
    val tableName = "labels:label_user_enterprise_vT"
    val columnFamily1 = "insureinfo"

    //累计增减员次数
    val ent_add_regulation_times_data = ent_add_regulation_times(ods_policy_detail, ods_policy_preserve_detail).distinct
    saveToHbase(ent_add_regulation_times_data, columnFamily1, "ent_add_regulation_times", conf_fs, tableName, conf)

    //月均增减员次数
    val ent_month_regulation_times_r = ent_month_regulation_times(ent_add_regulation_times_data, ent_summary_month_1).distinct
    saveToHbase(ent_month_regulation_times_r, columnFamily1, "ent_month_regulation_times", conf_fs, tableName, conf)

    //累计增员人数
    val ent_add_sum_persons_r = ent_add_sum_persons(ent_summary_month).distinct
    saveToHbase(ent_add_sum_persons_r, columnFamily1, "ent_add_sum_persons", conf_fs, tableName, conf)

    //累计减员人数
    val ent_del_sum_persons_r = ent_del_sum_persons(ent_summary_month).distinct()
    saveToHbase(ent_del_sum_persons_r, columnFamily1, "ent_del_sum_persons", conf_fs, tableName, conf)

    //月均在保人数
    val ent_month_plc_persons_r = ent_month_plc_persons(ent_summary_month_1).distinct
    saveToHbase(ent_month_plc_persons_r, columnFamily1, "ent_month_plc_persons", conf_fs, tableName, conf)

    //续投人数
    val ent_continuous_plc_persons_r = ent_continuous_plc_persons(ent_summary_month).distinct
    saveToHbase(ent_continuous_plc_persons_r, columnFamily1, "ent_continuous_plc_persons", conf_fs, tableName, conf)

    //投保工种数
    val ent_insure_craft_r = ent_insure_craft(ods_policy_detail, ods_policy_insured_detail).distinct
    saveToHbase(ent_insure_craft_r, columnFamily1, "ent_insure_craft", conf_fs, tableName, conf)

    //求出该企业中第一工种出现的类型哪个最多
    val ent_first_craft_r = ent_first_craft(ods_policy_insured_detail, ods_policy_detail, d_work_level).distinct()
    saveToHbase(ent_first_craft_r, columnFamily1, "ent_first_craft", conf_fs, tableName, conf)

    //求出该企业中第二工种出现的类型哪个最多
    val ent_second_craft_r = ent_second_craft(ods_policy_insured_detail, ods_policy_detail, d_work_level).distinct()
    saveToHbase(ent_second_craft_r, columnFamily1, "ent_second_craft", conf_fs, tableName, conf)

    //求出该企业中第三工种出现的类型哪个最多
    val ent_third_craft_r = ent_third_craft(ods_policy_insured_detail, ods_policy_detail, d_work_level).distinct()
    saveToHbase(ent_third_craft_r, columnFamily1, "ent_third_craft", conf_fs, tableName, conf)

    //该企业中哪个工种类型的赔额额度最高
    val ent_most_money_craft_r = ent_most_money_craft(ods_policy_insured_detail, ods_policy_detail, d_work_level, employer_liability_claims).distinct()
    saveToHbase(ent_most_money_craft_r, columnFamily1, "ent_most_money_craft", conf_fs, tableName, conf)

    //该企业中哪个工种类型出险最多
    val ent_most_count_craft_r = ent_most_count_craft(ods_policy_insured_detail: DataFrame, ods_policy_detail: DataFrame, employer_liability_claims: DataFrame).distinct()
    saveToHbase(ent_most_count_craft_r, columnFamily1, "ent_most_count_craft", conf_fs, tableName, conf)

    //投保人员占总人数比
    val insured_rate_r = insured_rate(ods_policy_detail: DataFrame, ods_policy_insured_detail: DataFrame, ent_sum_level: DataFrame).distinct()
    saveToHbase(insured_rate_r, columnFamily1, "insured_rate", conf_fs, tableName, conf)

    //有效保单数
    val effective_policy_r = effective_policy(ods_policy_detail).distinct()
    saveToHbase(effective_policy_r, columnFamily1, "effective_policy", conf_fs, tableName, conf)

    //累计投保人次(不对身份证去重)
    val total_insured_count_r = total_insured_count(ods_policy_detail, ods_policy_insured_detail).distinct()
    saveToHbase(total_insured_count_r, columnFamily1, "total_insured_count", conf_fs, tableName, conf)

    //累计投保人数 totalInsuredPersons（去重）对身份证号去重
    val total_insured_persons_r = total_insured_persons(ods_policy_detail, ods_policy_insured_detail).distinct()
    saveToHbase(total_insured_persons_r, columnFamily1, "total_insured_persons", conf_fs, tableName, conf)

    //当前在保人数 (对身份证去重，条件是该人人在企业中：insure_policy_status='1')
    //    val cur_insured_persons_r = cur_insured_persons(ods_policy_detail, ods_policy_insured_detail).distinct()
    //    toHbase(cur_insured_persons_r, columnFamily1, "cur_insured_persons", conf_fs, tableName, conf)

    //新的当前在保人数
    val cur_insured_persons_r = read_people_product(sqlContext: HiveContext, location_mysql_url: String, prop: Properties, location_mysql_url_dwdb: String).map(x => (x._1, x._2._2 + "", "cur_insured_persons"))
    saveToHbase(cur_insured_persons_r, columnFamily1, "cur_insured_persons", conf_fs, tableName, conf)

    //累计保费
    val total_premium_data = total_premium(ods_policy_detail).distinct()
    saveToHbase(total_premium_data, columnFamily1, "total_premium", conf_fs, tableName, conf)

    //月均保费
    val avg_month_premium_r = avg_month_premium(total_premium_data, ent_summary_month_1).distinct()
    saveToHbase(avg_month_premium_r, columnFamily1, "avg_month_premium", conf_fs, tableName, conf)

    //连续在保月份，都有哪个月
    val month_number = ent_continuous_plc_month_number(ent_summary_month_1).distinct()
    saveToHbase(month_number, columnFamily1, "month_number", conf_fs, tableName, conf)

    //连续在保月数
    //月份的增加和减少
    def month_add_jian(number: Int, filter_date: String): String = {
      //当前月份+1
      val sdf = new SimpleDateFormat("yyyyMM")
      val dt = sdf.parse(filter_date)
      val rightNow = Calendar.getInstance()
      rightNow.setTime(dt)
      rightNow.add(Calendar.MONTH, number)
      val dt1 = rightNow.getTime()
      val reStr = sdf.format(dt1)
      reStr
    }

    val ent_continuous_plc_month_r = ent_continuous_plc_month(ent_summary_month_1).map(x => {
      val tep_three = if (!x._2.contains("-") && x._2.length > 0) 1 else if (x._2.contains("-")) {
        //最近的一次断开的月份，得到连续在保月份
        val res = x._2.split("-").sorted
        val first_data = res(0)
        val final_data = res(res.length - 1)
        //2个日期相隔多少个月，包括开始日期和结束日期
        val get_res_day = getBeg_End_one_two_month(first_data, final_data)

        //找出最近的一次的连续日期
        val filter_date = if (res.length == get_res_day.length) month_add_jian(0, res(0))
        else {
          val res_end = get_res_day.filter(!res.contains(_)).reverse(0)
          month_add_jian(1, res_end)
        }
        //得到2个日期之间相隔多少个月
        val end_final: Int = getBeg_End_one_two_month(filter_date, final_data).length
        end_final
      } else 0
      (x._1, tep_three.toString, x._3)
    }).distinct()
    saveToHbase(ent_continuous_plc_month_r, columnFamily1, "ent_continuous_plc_month", conf_fs, tableName, conf)


    //首次投保至今月数
    val ent_fist_plc_month_r = ent_fist_plc_month(ods_policy_detail).distinct()
    saveToHbase(ent_fist_plc_month_r, columnFamily1, "ent_fist_plc_month", conf_fs, tableName, conf)


    //人均保费:累计保费/累计投保人数
    val before: RDD[(String, String)] = total_premium_data.map(x => (x._1, x._2)).cache
    val after: RDD[(String, String)] = total_insured_persons_r.map(x => (x._1, x._2))
    val avg_person_premium: RDD[(String, String, String)] = before.join(after).map(x => {
      val avg_premium = x._2._1.toDouble / x._2._2.toDouble
      (x._1, avg_premium.toString, "avg_person_premium")
    })
    saveToHbase(avg_person_premium, columnFamily1, "avg_person_premium", conf_fs, tableName, conf)


    //年均保费(月均保费*12)
    val avg_year_premium = avg_month_premium_r.map(x => {
      val year_premium = x._2.toDouble * 12
      (x._1, year_premium.toString, "avg_year_premium")
    })
    saveToHbase(avg_year_premium, columnFamily1, "avg_year_premium", conf_fs, tableName, conf)

    //当前生效保单数
    val cureffected_policy = ods_policy_detail.where("policy_status in('0','1','7','9','10') and ent_id!='' ").selectExpr("ent_id").map(x => (x.getAs[String]("ent_id"), 1)).reduceByKey(_ + _).map(x => (x._1, x._2.toString, "cureffected_policy"))
    saveToHbase(cureffected_policy, columnFamily1, "cureffected_policy", conf_fs, tableName, conf)
  }

  def main(args: Array[String]): Unit = {
    val confs = new SparkConf()
      .setAppName("wuYu")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
      .set("spark.sql.broadcastTimeout", "36000")
    //      .setMaster("local[2]")
    val lines_source = Source.fromURL(getClass.getResource("/config_scala.properties")).getLines.toSeq
    val location_mysql_url: String = lines_source(2).toString.split("==")(1)
    val location_mysql_url_dwdb: String = lines_source(10).toString.split("==")(1)
    val prop: Properties = new Properties

    val sc = new SparkContext(confs)
    val sqlContext: HiveContext = new HiveContext(sc)

    Insure(sqlContext: HiveContext, location_mysql_url: String, location_mysql_url_dwdb: String, prop: Properties)
    sc.stop()
  }
}
