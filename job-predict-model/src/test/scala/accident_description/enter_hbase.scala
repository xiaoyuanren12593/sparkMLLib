package accident_description

import java.util.Properties

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MK on 2018/6/26.
  */
object enter_hbase {
  val conf_spark = new SparkConf()
    .setAppName("wuYu")
  conf_spark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf_spark.registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
  conf_spark.set("spark.sql.broadcastTimeout", "36000")
    .setMaster("local[2]")

  val sc = new SparkContext(conf_spark)

  val sqlContext: HiveContext = new HiveContext(sc)


  def main(args: Array[String]): Unit = {

    val ods_policy_product_plan: DataFrame = sqlContext.sql("select * from odsdb_prd.ods_policy_product_plan").cache

    val url = "jdbc:mysql://172.16.11.105:3306/odsdb?user=odsuser&password=odsuser"

    val prop = new Properties()
    import sqlContext.implicits._
    //过滤出雇主与非雇主
    val ods_policy_detail = sqlContext.read.jdbc(url, "ods_policy_detail", prop).map(x => {
      (x.getAs[String]("insure_code"), x.getAs[String]("ent_id"))
    }).toDF("insure_code", "ent_id")

    //得到保单号
    val product_code = sqlContext.read.jdbc(url, "dim_product", prop).where("product_type_2='蓝领外包'").map(x => {
      x.getAs[String]("product_code")
    }).toDF("insure_code")
    val tep_one = ods_policy_detail.join(product_code, "insure_code").map(x => x.getAs[String]("ent_id")).collect


    /**
      * 第一步:创建一个JobConf
      **/
    //定义HBase的配置
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "172.16.11.106")

    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "labels:label_user_enterprise")

    val usersRDD = sc.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    )
    val result = usersRDD.map { x => {
      //baseinfo
      val ent_type = Bytes.toString(x._2.getValue("baseinfo".getBytes, "ent_type".getBytes))
      val register_time = Bytes.toString(x._2.getValue("baseinfo".getBytes, "register_time".getBytes))
      val ent_name = Bytes.toString(x._2.getValue("baseinfo".getBytes, "ent_name".getBytes))
      val ent_province = Bytes.toString(x._2.getValue("baseinfo".getBytes, "ent_province".getBytes))
      val ent_city = Bytes.toString(x._2.getValue("baseinfo".getBytes, "ent_city".getBytes))
      val ent_city_type = Bytes.toString(x._2.getValue("baseinfo".getBytes, "ent_city_type".getBytes))
      val ent_mansex_proportion = Bytes.toString(x._2.getValue("baseinfo".getBytes, "ent_mansex_proportion".getBytes))
      val ent_womansex_proportion = Bytes.toString(x._2.getValue("baseinfo".getBytes, "ent_womansex_proportion".getBytes))
      val ent_employee_age = Bytes.toString(x._2.getValue("baseinfo".getBytes, "ent_employee_age".getBytes))
      val ent_insure_code = Bytes.toString(x._2.getValue("baseinfo".getBytes, "ent_insure_code".getBytes))
      val ent_scale = Bytes.toString(x._2.getValue("baseinfo".getBytes, "ent_scale".getBytes))
      val ent_influence_level = Bytes.toString(x._2.getValue("baseinfo".getBytes, "ent_influence_level".getBytes))
      val ent_potential_scale = Bytes.toString(x._2.getValue("baseinfo".getBytes, "ent_potential_scale".getBytes))


      //insureinfo
      val ent_fist_plc_month = Bytes.toString(x._2.getValue("insureinfo".getBytes, "ent_fist_plc_month".getBytes))
      val ent_add_regulation_times = Bytes.toString(x._2.getValue("insureinfo".getBytes, "ent_add_regulation_times".getBytes))
      val ent_month_regulation_times = Bytes.toString(x._2.getValue("insureinfo".getBytes, "ent_month_regulation_times".getBytes))
      val ent_add_sum_persons = Bytes.toString(x._2.getValue("insureinfo".getBytes, "ent_add_sum_persons".getBytes))
      val ent_del_sum_persons = Bytes.toString(x._2.getValue("insureinfo".getBytes, "ent_del_sum_persons".getBytes))
      val ent_month_plc_persons = Bytes.toString(x._2.getValue("insureinfo".getBytes, "ent_month_plc_persons".getBytes))
      val ent_continuous_plc_month = Bytes.toString(x._2.getValue("insureinfo".getBytes, "ent_continuous_plc_month".getBytes))
      val ent_continuous_plc_persons = Bytes.toString(x._2.getValue("insureinfo".getBytes, "ent_continuous_plc_persons".getBytes))
      val industry_type = Bytes.toString(x._2.getValue("insureinfo".getBytes, "industry_type".getBytes))
      val employee_total_num = Bytes.toString(x._2.getValue("insureinfo".getBytes, "employee_total_num".getBytes))
      val insured_rate = Bytes.toString(x._2.getValue("insureinfo".getBytes, "insured_rate".getBytes))
      val effective_policy = Bytes.toString(x._2.getValue("insureinfo".getBytes, "effective_policy".getBytes))
      val cureffected_policy = Bytes.toString(x._2.getValue("insureinfo".getBytes, "cureffected_policy".getBytes))
      val total_insured_count = Bytes.toString(x._2.getValue("insureinfo".getBytes, "total_insured_count".getBytes))
      val cur_insured_persons = Bytes.toString(x._2.getValue("insureinfo".getBytes, "cur_insured_persons".getBytes))
      val total_premium = Bytes.toString(x._2.getValue("insureinfo".getBytes, "total_premium".getBytes))
      val avg_month_premium = Bytes.toString(x._2.getValue("insureinfo".getBytes, "avg_month_premium".getBytes))
      val total_insured_persons = Bytes.toString(x._2.getValue("insureinfo".getBytes, "total_insured_persons".getBytes))
      val avg_person_premium = Bytes.toString(x._2.getValue("insureinfo".getBytes, "avg_person_premium".getBytes))
      val avg_year_premium = Bytes.toString(x._2.getValue("insureinfo".getBytes, "avg_year_premium".getBytes))
      val ent_insure_craft = Bytes.toString(x._2.getValue("insureinfo".getBytes, "ent_insure_craft".getBytes))
      val ent_first_craft = Bytes.toString(x._2.getValue("insureinfo".getBytes, "ent_first_craft".getBytes))
      val ent_second_craft = Bytes.toString(x._2.getValue("insureinfo".getBytes, "ent_second_craft".getBytes))
      val ent_third_craft = Bytes.toString(x._2.getValue("insureinfo".getBytes, "ent_third_craft".getBytes))
      val ent_most_money_craft = Bytes.toString(x._2.getValue("insureinfo".getBytes, "ent_most_money_craft".getBytes))
      val ent_most_count_craft = Bytes.toString(x._2.getValue("insureinfo".getBytes, "ent_most_count_craft".getBytes))

      //claiminfo
      val report_num = Bytes.toString(x._2.getValue("claiminfo".getBytes, "report_num".getBytes))
      val claim_num = Bytes.toString(x._2.getValue("claiminfo".getBytes, "claim_num".getBytes))
      val death_num = Bytes.toString(x._2.getValue("claiminfo".getBytes, "death_num".getBytes))
      val disability_num = Bytes.toString(x._2.getValue("claiminfo".getBytes, "disability_num".getBytes))
      val worktime_num = Bytes.toString(x._2.getValue("claiminfo".getBytes, "worktime_num".getBytes))
      val nonworktime_num = Bytes.toString(x._2.getValue("claiminfo".getBytes, "nonworktime_num".getBytes))
      val pre_all_compensation = Bytes.toString(x._2.getValue("claiminfo".getBytes, "pre_all_compensation".getBytes))
      val pre_death_compensation = Bytes.toString(x._2.getValue("claiminfo".getBytes, "pre_death_compensation".getBytes))
      val pre_dis_compensation = Bytes.toString(x._2.getValue("claiminfo".getBytes, "pre_dis_compensation".getBytes))
      val pre_wt_compensation = Bytes.toString(x._2.getValue("claiminfo".getBytes, "pre_wt_compensation".getBytes))
      val pre_nwt_compensation = Bytes.toString(x._2.getValue("claiminfo".getBytes, "pre_nwt_compensation".getBytes))
      val all_compensation = Bytes.toString(x._2.getValue("claiminfo".getBytes, "all_compensation".getBytes))
      val overtime_compensation = Bytes.toString(x._2.getValue("claiminfo".getBytes, "overtime_compensation".getBytes))
      val charged_premium = Bytes.toString(x._2.getValue("claiminfo".getBytes, "charged_premium".getBytes))
      val avg_aging_cps = Bytes.toString(x._2.getValue("claiminfo".getBytes, "avg_aging_cps".getBytes))
      val avg_aging_claim = Bytes.toString(x._2.getValue("claiminfo".getBytes, "avg_aging_claim".getBytes))
      val avg_aging_risk = Bytes.toString(x._2.getValue("claiminfo".getBytes, "avg_aging_risk".getBytes))
      val worktype_0_count = Bytes.toString(x._2.getValue("claiminfo".getBytes, "worktype_0_count".getBytes))
      val worktype_1_count = Bytes.toString(x._2.getValue("claiminfo".getBytes, "worktype_1_count".getBytes))
      val worktype_2_count = Bytes.toString(x._2.getValue("claiminfo".getBytes, "worktype_2_count".getBytes))
      val worktype_3_count = Bytes.toString(x._2.getValue("claiminfo".getBytes, "worktype_3_count".getBytes))
      val worktype_4_count = Bytes.toString(x._2.getValue("claiminfo".getBytes, "worktype_4_count".getBytes))
      val worktype_5_count = Bytes.toString(x._2.getValue("claiminfo".getBytes, "worktype_5_count".getBytes))
      val worktype_6_count = Bytes.toString(x._2.getValue("claiminfo".getBytes, "worktype_6_count".getBytes))
      val worktype_7_count = Bytes.toString(x._2.getValue("claiminfo".getBytes, "worktype_7_count".getBytes))
      val ent_employee_increase = Bytes.toString(x._2.getValue("claiminfo".getBytes, "ent_employee_increase".getBytes))
      val ent_monthly_risk = Bytes.toString(x._2.getValue("claiminfo".getBytes, "ent_monthly_risk".getBytes))
      val ent_report_time = Bytes.toString(x._2.getValue("claiminfo".getBytes, "ent_report_time".getBytes))
      val ent_material_integrity = Bytes.toString(x._2.getValue("claiminfo".getBytes, "ent_material_integrity".getBytes))
      val ent_important_event_death = Bytes.toString(x._2.getValue("claiminfo".getBytes, "ent_important_event_death".getBytes))
      val ent_important_event_disable = Bytes.toString(x._2.getValue("claiminfo".getBytes, "ent_important_event_disable".getBytes))
      val ent_rejected_count = Bytes.toString(x._2.getValue("claiminfo".getBytes, "ent_rejected_count".getBytes))
      val ent_withdrawn_count = Bytes.toString(x._2.getValue("claiminfo".getBytes, "ent_withdrawn_count".getBytes))
      val avg_risk_period = Bytes.toString(x._2.getValue("claiminfo".getBytes, "avg_risk_period".getBytes))


      val baseinfo = s"$ent_name;$ent_type;$register_time;$ent_province;$ent_city;" +
        s"$ent_city_type;$ent_mansex_proportion;$ent_womansex_proportion;$ent_employee_age;$ent_insure_code;$ent_scale;$ent_influence_level;$ent_potential_scale"


      val insureinfo = s"$ent_fist_plc_month;$ent_add_regulation_times;$ent_month_regulation_times;" +
        s"$ent_add_sum_persons;$ent_del_sum_persons;$ent_month_plc_persons;" +
        s"$ent_continuous_plc_month;$ent_continuous_plc_persons;$industry_type;" +
        s"$employee_total_num;$insured_rate;$effective_policy;$cureffected_policy;" +
        s"$total_insured_count;$cur_insured_persons;$total_premium;$avg_month_premium;" +
        s"$total_insured_persons;$avg_person_premium;$avg_year_premium;$ent_insure_craft;" +
        s"$ent_first_craft;$ent_second_craft;$ent_third_craft;$ent_most_money_craft;$ent_most_count_craft"

      val claiminfo = s"$report_num;$claim_num;$death_num;$disability_num;" +
        s"$worktime_num;$nonworktime_num;$pre_all_compensation;$pre_death_compensation;" +
        s"$pre_dis_compensation;$pre_wt_compensation;$pre_nwt_compensation;" +
        s"$all_compensation;$overtime_compensation;$charged_premium;$avg_aging_cps;$avg_aging_claim;" +
        s"$avg_aging_risk;$worktype_0_count;$worktype_1_count;$worktype_2_count;$worktype_3_count;" +
        s"$worktype_4_count;$worktype_5_count;$worktype_6_count;$worktype_7_count;$ent_employee_increase;" +
        s"$ent_monthly_risk;$ent_report_time;$ent_material_integrity;" +
        s"$ent_important_event_death;$ent_important_event_disable;$ent_rejected_count;$ent_withdrawn_count;$avg_risk_period"

      val ent_id = Bytes.toString(x._2.getRow)
      val ends = if (tep_one.contains(ent_id)) "是雇主" else "非雇主"
      baseinfo.concat(";").concat(insureinfo).concat(";").concat(claiminfo).concat(";").concat(ends)
      //      ent_id
    }
    }
     .repartition(1).saveAsTextFile("C:\\Users\\a2589\\Desktop\\需求one\\enter_hbases3")
  }
}
