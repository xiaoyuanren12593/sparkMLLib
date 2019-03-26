package com.bzn.cLable

import java.util.Properties

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object CLabelRes {
  def main(args: Array[String]): Unit = {
    //得到标签数据
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf_spark = new SparkConf().setAppName(getClass.getName)
    conf_spark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf_spark.set("spark.sql.broadcastTimeout", "36000")
//      .setMaster("local[4]")

    val sc: SparkContext = new SparkContext(conf_spark)
    val sqlContext: HiveContext = new HiveContext(sc)
    //读取渠道表
    val lines_source = Source.fromURL(getClass.getResource("/config_scala.properties")).getLines.toSeq
    val location_mysql_url: String = lines_source(4).toString.split("==")(1)
    val location_mysql_url_dwdb: String = lines_source(6).toString.split("==")(1)
    val location_mysql_url_test: String = lines_source(7).toString.split("==")(1)
    val prop: Properties = new Properties

//    val open_other_policy_temp = sqlContext.read.jdbc(location_mysql_url_test,"open_other_policy_temp",prop)
//      .select("insured_name","insured_cert_no","insured_mobile","product_code")
//    open_other_policy_temp.insertInto("odsdb_prd.open_other_policy_temp",overwrite = true)

    val open_other_policy_temp = sqlContext.read.jdbc(location_mysql_url_dwdb,"dim_product",prop)
      .select("product_code","product_new_1","product_new_2").filter("product_code is not null")
      .show(1000)
    //    val resOne = test_ofo_emp_enter_res.distinct().map(x => {
//      val c = x.getAs[String]("_c0")
//      (c)
//    }).map(x => {
//      val split = x.toString.split(",")
//      val len: Int = split.length
//      (len,x)
//    }).sortByKey(false)
//      .map(x => x._2)
//      .map(x => {
//        val split =  x.split(",")
//      })
//    resOne.foreach(println)
    sc.stop()
  }
}
