package com.bzn.cLable

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/***
  * 合并雇主、接口、ofo、58速运的数据
  * create user xingyuan
  * date 2019-4-4
  */
object MergeOfoEmpInter {

  def main(args: Array[String]): Unit = {
    //得到标签数据
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf_spark = new SparkConf().setAppName(getClass.getName)
    conf_spark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf_spark.set("spark.sql.broadcastTimeout", "36000")
//          .setMaster("local[4]")

    val sc: SparkContext = new SparkContext(conf_spark)
    val sqlContext: HiveContext = new HiveContext(sc)
    //读取渠道表
    val lines_source = Source.fromURL(getClass.getResource("/config_scala.properties")).getLines.toSeq
    val location_mysql_url: String = lines_source(4).toString.split("==")(1)
    val location_mysql_url_dwdb: String = lines_source(6).toString.split("==")(1)
    val location_mysql_url_test: String = lines_source(7).toString.split("==")(1)
    val prop: Properties = new Properties

    val empData = getEmpData(sqlContext)
//    empData.show(1000)

    val interData = getInterData(sqlContext)
//    interData.show(1000)

    val ofoData = getOfoData(sqlContext)

    val weddingData = getWeddingData(sqlContext)

    val clinetData = get58ClinetData(sqlContext)

    val courierData = get58CourierData(sqlContext)

    val empInterOfoData2Hive = empData.unionAll(interData).unionAll(ofoData).unionAll(weddingData).unionAll(clinetData).unionAll(courierData)
      .distinct()

    val outputTmpDir = "/share/cTable_emp_inter_ofo"
    val output = "odsdb_prd.emp_inter_ofo"

    empInterOfoData2Hive.rdd.map(x => x.mkString("\001")).repartition(1).saveAsTextFile(outputTmpDir)
    sqlContext.sql(s"""load data  inpath '$outputTmpDir' overwrite into table $output""")

    sc.stop()
  }

  def getOfoData(sqlContext:SQLContext) = {
    import  sqlContext.implicits._
    val open_other_policy_temp = sqlContext.sql("select insured_name,insured_cert_no,insured_mobile,'ofo' from odsdb_prd.open_ofo_policy_parquet")
      .toDF("insured_name","insured_cert_no","insured_mobile","product_type")
      .map(x => {
        val insured_name = x.getAs[String]("insured_name")
        var insured_cert_no = x.getAs[String]("insured_cert_no")
        var insured_mobile = x.getAs[String]("insured_mobile")
        val product_type = x.getAs[String]("product_type")
        if(insured_cert_no == ""){
          insured_cert_no =  null
        }
        if(insured_mobile == ""){
          insured_mobile =  null
        }
        (insured_name,insured_cert_no,insured_mobile,product_type)
      })
      .toDF("insured_name","insured_cert_no","insured_mobile","product_type")
      .distinct()
    open_other_policy_temp
  }
  /**
    * 得到接口的数据
    * @param sqlContext
    * @return
    */
  def getInterData(sqlContext:SQLContext) = {
    import sqlContext.implicits._
    val open_other_policy_temp = sqlContext.sql("select insured_name,insured_cert_no,insured_mobile,product_code from odsdb_prd.open_other_policy_temp")
      .filter("length(product_code) > 0")

    val dim_product_temp = sqlContext.sql("select product_code,product_type from odsdb_prd.dim_product_temp")
      .filter("length(product_code) > 0")

    val res = open_other_policy_temp.join(dim_product_temp,"product_code")
      .select("insured_name","insured_cert_no","insured_mobile","product_type")
      .map(x => {
        val insured_name = x.getAs[String]("insured_name")
        var insured_cert_no = x.getAs[String]("insured_cert_no")
        var insured_mobile = x.getAs[String]("insured_mobile")
        val product_type = x.getAs[String]("product_type")
        if(insured_cert_no == ""){
          insured_cert_no =  null
        }
        if(insured_mobile.length == ""){
          insured_mobile =  null
        }
        (insured_name,insured_cert_no,insured_mobile,product_type)
      })
      .toDF("insured_name","insured_cert_no","insured_mobile","product_type")
      .distinct()
     res
  }

  /**
    * 得到雇主数据
    * @param sqlContext
    * @return
    */
  def getEmpData(sqlContext:SQLContext) = {
    import sqlContext.implicits._
    val ods_policy_insured_detail = sqlContext.sql("select insured_name ,insured_cert_no ,insured_mobile,policy_id from odsdb_prd.ods_policy_insured_detail")
        .filter("length(policy_id) > 0")
      .distinct()

    val ods_policy_detail = sqlContext.sql("select insure_code,policy_id from odsdb_prd.ods_policy_detail")
      .filter("length(policy_id) > 0")
      .distinct()

    val dim_product_temp = sqlContext.sql("select product_code,product_type from odsdb_prd.dim_product_temp")
      .filter("length(product_code) > 0")

    val resOne = ods_policy_insured_detail.join(ods_policy_detail,"policy_id")

    val res = resOne.join(dim_product_temp,resOne("insure_code") === dim_product_temp("product_code"))
      .select("insured_name","insured_cert_no","insured_mobile","product_type")
      .map(x => {
        val insured_name = x.getAs[String]("insured_name")
        var insured_cert_no = x.getAs[String]("insured_cert_no")
        var insured_mobile = x.getAs[String]("insured_mobile")
        val product_type = x.getAs[String]("product_type")
        if(insured_cert_no == ""){
          insured_cert_no =  null
        }
        if(insured_mobile == ""){
          insured_mobile =  null
        }
        (insured_name,insured_cert_no,insured_mobile,product_type)
      })
      .toDF("insured_name","insured_cert_no","insured_mobile","product_type")
      .distinct()
    res
  }

  /**
    * 得到婚礼纪数据
    * @param sqlContext
    * @return
    */
  def getWeddingData(sqlContext: SQLContext) = {
    import sqlContext.implicits._

    val open_insured = sqlContext.sql("select name, cert_no, tel,'婚礼纪' from odsdb_prd.open_insured")
      .toDF("insured_name","insured_cert_no","insured_mobile","product_type")
      .map(x => {
        val insured_name = x.getAs[String]("insured_name")
        var insured_cert_no = x.getAs[String]("insured_cert_no")
        var insured_mobile = x.getAs[String]("insured_mobile")
        val product_type = x.getAs[String]("product_type")
        if(insured_cert_no == ""){
          insured_cert_no =  null
        }
        if(insured_mobile == ""){
          insured_mobile =  null
        }
        (insured_name,insured_cert_no,insured_mobile,product_type)
      })
      .toDF("insured_name","insured_cert_no","insured_mobile","product_type")
      .distinct()
    open_insured
  }

  /**
    * 58客户
    * @param sqlContext
    * @return
    */
  def get58ClinetData (sqlContext: SQLContext) = {
    import sqlContext.implicits._
    val client58 = sqlContext.sql("select '','', client_mobile ,'58速运-客户' from odsdb_prd.open_express_policy_all_d")
      .toDF("insured_name","insured_cert_no","insured_mobile","product_type")
      .map(x => {
        val insured_name = x.getAs[String]("insured_name")
        var insured_cert_no = x.getAs[String]("insured_cert_no")
        var insured_mobile = x.getAs[String]("insured_mobile")
        val product_type = x.getAs[String]("product_type")
        if(insured_cert_no == ""){
          insured_cert_no =  null
        }
        if(insured_mobile == ""){
          insured_mobile =  null
        }
        (insured_name,insured_cert_no,insured_mobile,product_type)
      })
      .toDF("insured_name","insured_cert_no","insured_mobile","product_type")
      .distinct()
    client58
  }

  /**
    * 58司机
    * @param sqlContext
    * @return
    */
  def get58CourierData(sqlContext : SQLContext) ={
    import sqlContext.implicits._
    val courier58 = sqlContext.sql("select courier_name,courier_card_no, courier_mobile ,'58速运-司机' from odsdb_prd.open_express_policy_all_d")
      .toDF("insured_name","insured_cert_no","insured_mobile","product_type")
      .map(x => {
      val insured_name = x.getAs[String]("insured_name")
      var insured_cert_no = x.getAs[String]("insured_cert_no")
      var insured_mobile = x.getAs[String]("insured_mobile")
      val product_type = x.getAs[String]("product_type")
      if(insured_cert_no == ""){
        insured_cert_no =  null
      }
      if(insured_mobile == ""){
        insured_mobile =  null
      }
      (insured_name,insured_cert_no,insured_mobile,product_type)
    })
      .toDF("insured_name","insured_cert_no","insured_mobile","product_type")
      .distinct()
    courier58
  }
}
