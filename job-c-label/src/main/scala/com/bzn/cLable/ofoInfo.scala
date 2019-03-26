package com.bzn.cLable

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import sun.util.calendar.CalendarUtils.mod

object ofoInfo {
  def main(args: Array[String]): Unit = {
    //得到标签数据
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf_spark = new SparkConf().setAppName(getClass.getName)
    conf_spark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf_spark.set("spark.sql.broadcastTimeout", "36000")
    //      .setMaster("local[4]")

    val sc: SparkContext = new SparkContext(conf_spark)
    val sqlContext: HiveContext = new HiveContext(sc)
    import sqlContext.implicits._

//    val ofoInfoData = getofoInfoData(sqlContext:SQLContext)
    val infoOfoCert = sqlContext.sql("select insured_cert_no,time_id,cnt from odsdb_prd.info_ofo_cert")
        .where("insured_cert_no is not null")
      .map(x=>{
        val insured_cert_no = x.getAs[String]("insured_cert_no")
        val time_id = x.getAs[String]("time_id")
        val cnt = x.getAs[Long]("cnt")
        (insured_cert_no,(time_id,cnt))
      })
        .reduceByKey((x1,x2)=> {
          if (x1._2.toLong > x2._2.toLong) x1 else x2
        })
        .map(x => {
          (x._1,x._2._1,x._2._2)
        })
      .toDF("insured_cert_no","time_id","cnt")
//    val runDay= sqlContext.sql("select insured_cert_no from odsdb_prd.open_ofo_policy_parquet")
//      .where("length(insured_cert_no) = 18")
//      .map(x => x.getAs[String]("insured_cert_no"))
//      .filter(x => cardCodeVerifySimple(x))
//      .map(x => {
//        (x,1)
//      }).reduceByKey(_+_)
//      .toDF("insured_cert_no","runDay")
//
    val outputTmpDir = "/share/info_ofo_cert_res"
    val output = "odsdb_prd.info_ofo_cert_res"

    infoOfoCert.rdd.map(x => x.mkString("\001")).repartition(1).saveAsTextFile(outputTmpDir)
    sqlContext.sql(s"""load data  inpath '$outputTmpDir' overwrite into table $output""")
    sc.stop()
  }

  def getofoInfoData(sqlContext:SQLContext) = {
    import sqlContext.implicits._
    val ofoInfoData = sqlContext.sql("select insured_cert_no from odsdb_prd.open_ofo_policy_parquet")
      .where("length(insured_cert_no) = 18")
      .distinct()
    val res= ofoInfoData.map(x => x.getAs[String]("insured_cert_no"))
      .filter(x => cardCodeVerifySimple(x))
      .map(insured_cert_no => {
        val nation = insured_cert_no.substring(0,4)
        val year = insured_cert_no.substring(6,10)
        val monthDay = insured_cert_no.substring(10,14)
        val sexInt = insured_cert_no.substring(insured_cert_no.length-2,insured_cert_no.length-1).toInt
        val sex: Int = mod(sexInt, 2)
        var sexRes = "女"
        if(sex == 1){
          sexRes = "男"
        }
        (insured_cert_no,nation,year,monthDay,sexRes)
      })
      .toDF("insured_cert_no","nation","year","monthDay","sex")
    val outputTmpDir = "/share/ofo_info"
    val output = "odsdb_prd.ofo_info"

    res.rdd.map(x => x.mkString("\001")).repartition(1).saveAsTextFile(outputTmpDir)
    sqlContext.sql(s"""load data  inpath '$outputTmpDir' overwrite into table $output""")
  }
  /**
    * 身份证校验
    * @param cardcode
    * @return
    */
  private def cardCodeVerifySimple(cardcode: String): Boolean = {
    val isIDCard1 = "^[1-9]\\d{7}((0\\d)|(1[0-2]))(([0|1|2]\\d)|3[0-1])\\d{3}$"
    //第二代身份证正则表达式(18位)
    val isIDCard2 = "^[1-9]\\d{5}[1-9]\\d{3}((0\\d)|(1[0-2]))(([0|1|2]\\d)|3[0-1])((\\d{4})|\\d{3}[A-Z])$"
    //验证身份证
    if (cardcode.matches(isIDCard1) || cardcode.matches(isIDCard2))
      return true
    false
  }
}
