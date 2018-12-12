package company.hbase_label.enterprise

import java.util.Properties

import company.hbase_label.enterprise.Ent_insureinfo.toHbase
import company.hbase_label.enterprise.enter_until.Insureinfo_until
import company.hbase_label.until
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object Ent_insureinfo_test extends Insureinfo_until with until {

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

    Insure(sqlContext: HiveContext, location_mysql_url: String, location_mysql_url_dwdb: String, prop: Properties)
    sc.stop()
  }
}
