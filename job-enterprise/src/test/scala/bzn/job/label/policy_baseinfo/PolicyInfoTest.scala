package bzn.job.label.policy_baseinfo

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object PolicyInfoTest extends PolicyUntilTest {

  //订单企业ID
  def policy_ent_id(ods_policy_detail: DataFrame, ent_enterprise_info: DataFrame): RDD[(String, String, String)] = {
    //user_id:用户信息表ID
    //找到我该企业的ID和
    val tep_one = ods_policy_detail.filter("LENGTH(policy_code)>0").selectExpr("policy_code", "ent_id as user_id")
    val tep_two = ent_enterprise_info.selectExpr("id", "id as user_id", "create_time")
    //你是以user_id进行的join,假如说另外一张表的user_id中也有空，我这张表中也有空，那么就会空对空，发生笛卡儿积
    val tepThree = tep_one.join(tep_two, "user_id").filter("length(user_id)>1 and length(create_time) > 12")
    //    |             user_id|         policy_code|                  id|        create_time|
    //    |3c95bb8f25534f1d9...|PEAC20171102Q000E...|71887c89d2bb4a20b...|2017-12-22 17:05:19|
    val end: RDD[(String, String, String)] = tepThree
      .map(x => {
        //保单号|企业ID
        val create_time = x.getAs("create_time").toString
        (x.getString(1), (x.getString(2), currentTimeL(create_time)))
      })
      .reduceByKey((x1, x2) => {
        //取得最近日期的数据
        val res = if (x1._2 > x2._2) x1 else x2
        res
      })
      //保单号|企业ID
      .map(x => (x._1, x._2._1, "policy_ent_id"))

    end
  }

  def Policy(sqlContext: HiveContext): Unit = {
    //HBaseConf
    val conf = HbaseConf("policy_information_vT")._1
    val conf_fs = HbaseConf("policy_information_vT")._2
    val tableName = "policy_information_vT"
    val columnFamily1 = "baseinfo"

    //ent_enterprise_info	官网企业信息表
    val ods_policy_detail: DataFrame = sqlContext.sql("select * from odsdb_prd.ods_policy_detail").cache()
    val ent_enterprise_info = sqlContext.sql("select * from odsdb_prd.ent_enterprise_info").cache()
    val pdt_product_sku = sqlContext.sql("select * from odsdb_prd.pdt_product_sku").cache()

    //订单企业ID
    val policy_ent_id_r = policy_ent_id(ods_policy_detail, ent_enterprise_info).distinct()
    policy_ent_id_r.take(10).foreach(println)
//    toHbase(policy_ent_id_r, columnFamily1, "policy_ent_id", conf_fs, tableName, conf)

    //产品编号
    val policy_insure_code_r = policy_insure_code(ods_policy_detail).distinct()
    policy_insure_code_r.take(10).foreach(println)
//    toHbase(policy_insure_code_r, columnFamily1, "policy_insure_code", conf_fs, tableName, conf)

    //保单生效时间
    val policy_start_date_r = policy_start_date(ods_policy_detail).distinct()
    policy_start_date_r.take(10).foreach(println)
//    toHbase(policy_start_date_r, columnFamily1, "policy_start_date", conf_fs, tableName, conf)

    //保单截至时间
    val policy_end_date_r = policy_end_date(ods_policy_detail).distinct()
    policy_end_date_r.take(10).foreach(println)
//    toHbase(policy_end_date_r, columnFamily1, "policy_end_date", conf_fs, tableName, conf)

    //保费
    val policy_premium_r = policy_premium(ods_policy_detail).distinct()
    policy_premium_r.take(10).foreach(println)
//    toHbase(policy_premium_r, columnFamily1, "policy_premium", conf_fs, tableName, conf)

    //保单状态
    val policy_status_r = policy_status(ods_policy_detail).distinct()
    policy_status_r.take(10).foreach(println)
//    toHbase(policy_status_r, columnFamily1, "policy_status", conf_fs, tableName, conf)

    //更新时间
    val policy_update_time_r = policy_update_time(ods_policy_detail).distinct()
    policy_update_time_r .take(10).foreach(println)
//    toHbase(policy_update_time_r, columnFamily1, "policy_update_time", conf_fs, tableName, conf)

    //保额
    val policy_term_three_r = policy_term_three(ods_policy_detail, pdt_product_sku).distinct()
    policy_term_three_r.take(10).foreach(println)
//    toHbase(policy_term_three_r, columnFamily1, "policy_term_three", conf_fs, tableName, conf)
  }

  def main(args: Array[String]): Unit = {

    val conf_s = new SparkConf().setAppName("wuYu")
    conf_s.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf_s.registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
    conf_s.set("spark.sql.broadcastTimeout", "36000")
          .setMaster("local[2]")

    val sc = new SparkContext(conf_s)
    val sqlContext: HiveContext = new HiveContext(sc)

    Policy(sqlContext: HiveContext)
    sc.stop()
  }
}
