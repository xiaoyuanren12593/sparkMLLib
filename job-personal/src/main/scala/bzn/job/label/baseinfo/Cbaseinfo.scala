package bzn.job.label.baseinfo


import bzn.job.common.Until
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

//所在单位
object Cbaseinfo extends Until {
  //只用到了官网的表
  //所在单位
  def user_company(ods_policy_insured_detail: DataFrame): RDD[(String, String, String)] = {
    val end: RDD[(String, String, String)] = ods_policy_insured_detail
      .filter("LENGTH(insured_company_name) > 0 and LENGTH(insured_cert_no) > 0")
      .select("insured_cert_no", "insured_company_name")
      .map(x => (x.getString(0), x.getString(1)))
      .reduceByKey((x1, x2) => x1)
      .map(x => (x._1, x._2, "user_company"))

    end
  }

  //查看该投保人有没有子女
  def user_child(ods_policy_insured_detail: DataFrame): RDD[(String, String, String)] = {
    val rdd = ods_policy_insured_detail
      .filter("LENGTH(insured_cert_no) > 4 and LENGTH(child_cert_no)>4")
      .select("insured_cert_no", "child_cert_no")
    val end = rdd
      .map(x => x.getString(0))
      .distinct
      .map(x => (x, "是", "user_child"))

    end
  }

  //C端用户工种级别 (C端是个体户，B端是企业)
  def user_craft_level(d_work_level: DataFrame, ods_policy_insured_detail: DataFrame): RDD[(String, String, String)] = {
    //也就是找到我该表中的每个人对应的是哪种等级
    //work_type:工种类型
    val tepOne = d_work_level.select("work_type", "ai_level")
    val tepTwo = ods_policy_insured_detail.select("insured_cert_no", "insured_work_type")
    val tepThree = tepOne.join(tepTwo, tepOne("work_type") === tepTwo("insured_work_type"))
      .filter("ai_level is NOT NULL and insured_work_type is not null and length(insured_cert_no)>0")
    //    |work_type|ai_level|   insured_cert_no|insured_work_type|
    //    |    车间操作工|       3|222402197008140614|            车间操作工|
    val end: RDD[(String, String, String)] = tepThree
      //因为身份证号码是唯一的，因此我去重的话，只取一个即可
      .map(x => (x.get(2).toString, x.get(1) + ""))
      .reduceByKey((x1, x2) => x1)
      .map(x => (x._1, x._2, "user_craft_level"))

    end
  }

  //C端用户企业ID
  def user_ent_id(ods_policy_detail: DataFrame, ods_policy_insured_detail: DataFrame): RDD[(String, String, String)] = {
    //每个投保人，对应的企业ID是什么
    val tepOne = ods_policy_detail.filter("length(ent_id) > 0").select("policy_id", "ent_id")
    val tepTwo = ods_policy_insured_detail.filter("length(insured_cert_no) > 0").select("policy_id", "insured_cert_no")

    val tepThree = tepOne.join(tepTwo, "policy_id")
    //    |           policy_id|              ent_id|   insured_cert_no|
    //    |cc1025c8708746dbb...|365104828040476c9...|222402197008140614|
    val end: RDD[(String, String, String)] = tepThree
      .map(x => (x.getString(2), x.getString(1)))
      .reduceByKey((x1, x2) => x1)
      .map(x => (x._1, x._2, "user_ent_id"))

    end
  }

  //单人报案次数(已有了个人报案件数，因此可去掉)
  def user_report_num(employer_liability_claims: DataFrame): RDD[(String, String, String)] = {
    //cert_no:被保人身份证号
    val tepOne = employer_liability_claims
      .filter("length(cert_no)>0")
      .select("cert_no")
      .map(_.getString(0))
    val end: RDD[(String, String, String)] = tepOne
      .map(x => (x, 1))
      .reduceByKey(_ + _)
      .map(x => (x._1, x._2 + "", "user_report_num"))

    end
  }

  //出险周期
  def user_aging_risk(ods_policy_risk_period: DataFrame): RDD[(String, String, String)] = {
    val end: RDD[(String, String, String)] = ods_policy_risk_period
      .filter("LENGTH(cert_no)>0 and risk_period <3")
      .select("cert_no", "risk_period")
      .map(x => (x.getString(0), 1))
      .reduceByKey(_ + _)
      .map(x => (x._1, x._2 + "", "user_aging_risk"))

    end
  }

  //user_insure_coverage:保障情况(官网)
  def user_insure_coverage(sqlContext: HiveContext, before: DataFrame, end_result: DataFrame): RDD[(String, String, String)] = {
    val after = sqlContext.sql("select * from odsdb_prd.ods_policy_insured_detail")
      .filter("LENGTH(insured_cert_no)=18")
      .select("policy_id", "insured_cert_no")
    val j_after: RDD[(String, String)] = before
      .join(after, "policy_id")
      .select("insured_cert_no", "sku_coverage")
      .map(x => (x.getString(0), x.getString(1)))
      .reduceByKey((x1, x2) => {
        val res = if (x1 != x2) x1 else x1
        res
      })

    val end = j_after.map(x => (x._1, x._2, "user_insure_coverage"))
    end
  }

  // official_website_coverage:保障情况:官网
  def official_website_coverage(sqlContext: HiveContext, ods_policy_detail: DataFrame): RDD[(String, String, String)] = {
    val after = sqlContext.sql("select * from odsdb_prd.ods_policy_insured_detail")
      .filter("LENGTH(insured_cert_no)=18")
      .select("policy_id", "insured_cert_no")
    val j_after: RDD[(String, String)] = ods_policy_detail
      .join(after, "policy_id")
      .select("insured_cert_no", "sku_coverage")
      .map(x => (x.getString(0), x.getString(1)))
      .reduceByKey((x1, x2) => {
        val res = if (x1 >= x2) x1 else x2
        res
      })
    val end = j_after
      .map(x => {
        val bz = if (x._2 != null ) x._2 else "null"
        (x._1, bz, " official_website_coverage")
      })

    end
  }

  def main(args: Array[String]): Unit = {
    val conf_s = new SparkConf().setAppName("wuYu")
      //      .setMaster("local[2]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
      .set("spark.sql.broadcastTimeout", "36000")
      .set("spark.network.timeout", "36000")

    val sc = new SparkContext(conf_s)
    val sqlContext: HiveContext = new HiveContext(sc)

    //odr_policy_insured	官网被保人清单表
    //它的写法是将2张表的信息写到了一起
    val ods_policy_insured_detail = sqlContext.sql("select * from odsdb_prd.ods_policy_insured_detail")
    //企业表
    val ods_policy_detail = sqlContext.sql("select * from odsdb_prd.ods_policy_detail").cache
    //d_work_level	工种风险等级码表
    val d_work_level = sqlContext.sql("select * from odsdb_prd.d_work_level").cache

    //odr_policy_insured	官网被保人清单表
    //employer_liability_claims	雇主保理赔记录表
    val employer_liability_claims = sqlContext.sql("select * from odsdb_prd.employer_liability_claims").cache
    val ods_policy_risk_period = sqlContext.sql("select * from odsdb_prd.ods_policy_risk_period").cache

    val conf = HbaseConf("labels:label_user_personal_vT")._1
    val conf_fs = HbaseConf("labels:label_user_personal_vT")._2
    val tableName = "labels:label_user_personal_vT"
    val columnFamily1 = "baseinfo"

    //所在单位
    val user_company_rs = user_company(ods_policy_insured_detail)
    toHbase(user_company_rs, columnFamily1, "user_company", conf_fs, tableName, conf)

    //查看该投保人有没有子女
    val user_child_rs = user_child(ods_policy_insured_detail)
    toHbase(user_child_rs, columnFamily1, "user_child", conf_fs, tableName, conf)

    //C端用户工种级别 (C端是个体户，B端是企业)
    val user_craft_level_rs = user_craft_level(d_work_level, ods_policy_insured_detail)
    toHbase(user_craft_level_rs, columnFamily1, "user_craft_level", conf_fs, tableName, conf)

    //C端用户企业ID
    val user_ent_id_rs = user_ent_id(ods_policy_detail, ods_policy_insured_detail)
    toHbase(user_ent_id_rs, columnFamily1, "user_ent_id", conf_fs, tableName, conf)

    //单人报案次数
    val user_report_num_rs = user_report_num(employer_liability_claims: DataFrame)
    toHbase(user_report_num_rs, columnFamily1, "user_report_num", conf_fs, tableName, conf)

    //出险周期
    //统计单人出险周期小于3的保单,出现的个数
    val user_aging_risk_rs = user_aging_risk(ods_policy_risk_period)
    toHbase(user_aging_risk_rs, columnFamily1, "user_aging_risk", conf_fs, tableName, conf)

    //official_website_coverage:保障情况:官网
    val official_website_coverage_rs = official_website_coverage(sqlContext, ods_policy_detail)
    toHbase(official_website_coverage_rs, columnFamily1, "official_website_coverage", conf_fs, tableName, conf)

    //user_now_efficient_singular:当前生效保单数,使用官网计算
    val user_now_efficient_singular_rs = ods_policy_insured_detail.filter("length(insured_cert_no)=18").select("insured_cert_no").map(x => (x.getString(0), 1)).reduceByKey(_ + _).map(x => (x._1, x._2 + "", "user_now_efficient_singular"))
    toHbase(user_now_efficient_singular_rs, columnFamily1, "user_now_efficient_singular", conf_fs, tableName, conf)
  }
}
