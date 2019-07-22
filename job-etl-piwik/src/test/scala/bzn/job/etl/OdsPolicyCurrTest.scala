package bzn.job.etl

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import bzn.job.common.Until
import bzn.job.etl.OdsPolicyCurr.{getBeg_End_one_two, getClass, readMysqlTable}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext

import scala.io.Source

object  OdsPolicyCurrTest extends Until {

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName
    val sparkConf: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName, "local[*]")

    val sc = sparkConf._2
    val sqlContext: SQLContext = sparkConf._4

    val OdsPolicyCurr: DataFrame = odsPolicyCurr(sqlContext)
//    OdsPolicyCurr.write.mode(SaveMode.Overwrite).saveAsTable("odsdb.ods_policy_curr")
    OdsPolicyCurr.printSchema()
    sc.stop()

  }

  def odsPolicyCurr(sqlContext: SQLContext): DataFrame = {

    import sqlContext.implicits._
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      (date + "")
    })
    sqlContext.udf.register("getNull", () => "")

    /**
      * 读取产品表  读取dwdb.dim_product
      */
    val dimProduct: DataFrame = sqlContext
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://172.16.11.105:3306/dwdb?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "bzn@cdh123!")
      .option("numPartitions","10")
      .option("partitionColumn","id")
      .option("lowerBound", "0")
      .option("upperBound","200")
      .option("dbtable", "dim_product")
      .load()
      .where("product_type_2 = '蓝领外包'")
      .selectExpr("product_code")

    /**
      * 读取保单信息
      */
    val odsPolicyDetail: DataFrame = readMysqlTable(sqlContext, "ods_policy_detail")
      .selectExpr("policy_code", "policy_id", "start_date", "end_date", "insure_code")
    //2019-07-01 00:00:00 2020-06-30 23:59:59

    /**
      * 保单起始、结束信息
      */
    val policyTemp: DataFrame = odsPolicyDetail
      .join(dimProduct, odsPolicyDetail("insure_code") === dimProduct("product_code"), "inner")
      .where("policy_code is not null")
      .selectExpr("policy_code", "policy_id", "start_date", "end_date")

    /**
      * 保单id信息，用来筛选在保人信息
      */
    val policyTemp2: DataFrame = policyTemp.selectExpr("policy_id")

    /**
      * 每张保单的每日有效情况
      */
    val policyDays: DataFrame = policyTemp
      .mapPartitions(rdd => {
        rdd.flatMap(x => {
          val policyCode = x.getAs[String]("policy_code")
          val policyId = x.getAs[String]("policy_id")
          val startTimeStamp = x.getAs[java.sql.Timestamp]("start_date")
          var startDate = ""
          if (startTimeStamp != null) {
            startDate = startTimeStamp.toString.split(" ")(0).replaceAll("-", "").replaceAll("/", "")
          }
          val endTimeStamp = x.getAs[java.sql.Timestamp]("end_date")
          var endDate = ""
          if (endTimeStamp != null) {
            endDate = endTimeStamp.toString.split(" ")(0).replaceAll("-", "").replaceAll("/", "")
          }
          val res = getBeg_End_one_two(startDate, endDate).map(day_id => {
            (day_id, policyId, policyCode)
          })
          res
        })
      })
      .toDF("day_id", "policy_id", "policy_code")

    /**
      * 读取在保人明细表
      */
    sqlContext.sql("use odsdb")
    val odsPolicyInsuredDetail: DataFrame = sqlContext.sql("select policy_id as policy_id_insured, insured_start_date,insured_end_date ,insured_cert_no " +
      "from odsdb_prd.ods_policy_insured_detail where insured_start_date is not null and insured_end_date is not null")

    /**
      * 筛选在保人信息
      */
    val odsPolicyInsuredDetailSelect: DataFrame = odsPolicyInsuredDetail.join(policyTemp2, odsPolicyInsuredDetail("policy_id_insured") === policyTemp2("policy_id"))
      .selectExpr("policy_id", "insured_start_date", "insured_end_date", "insured_cert_no")

    /** x
      * 每张保单每天在保人明细
      */
    val InsuredDayRes = odsPolicyInsuredDetailSelect
      .mapPartitions(rdd => {
        rdd.flatMap(x => {
          val policyId = x.getAs[String]("policy_id")
          var insuredStartDate = x.getAs[String]("insured_start_date").split(" ")(0).replaceAll("-", "").replaceAll("/", "")
          var insuredEndDate = x.getAs[String]("insured_end_date").split(" ")(0).replaceAll("-", "").replaceAll("/", "")
          val insuredCertNo = x.getAs[String]("insured_cert_no")
          val res = getBeg_End_one_two(insuredStartDate, insuredEndDate).map(day_id => {
            (policyId, day_id, insuredCertNo)
          })
          res
        })
      })
      .toDF("policy_id", "day_id", "insured_cert_no")

    /**
      * 每张保单的每日在保人数
      */
    val result: DataFrame = policyDays.join(InsuredDayRes, Seq("policy_id", "day_id"), "leftouter")
      .selectExpr("policy_id","policy_code","day_id","insured_cert_no")
      .map(x => {
        val policyId = x.getAs[String]("policy_id")
        val policyCode = x.getAs[String]("policy_code")
        val dayId = x.getAs[String]("day_id")
        var insuredCertNo = x.getAs[String]("insured_cert_no")
        if (insuredCertNo == null) {
          insuredCertNo = "`"
        }
        ((policyId, policyCode, dayId), insuredCertNo)
      })
      .groupByKey()
      .map(x => {
        var res: Int = 0
        val ls = x._2.toSet
        if (!ls.isEmpty) {
          res = ls.size
        }
        if (ls.contains("`")) {
          if (res != 0) {
            res = res - 1
          }
        }
        (x._1._3, x._1._1, x._1._2, res)
      })
      .toDF("day_id", "policy_id", "policy_code", "curr_insured")
      .selectExpr("getUUID() as id", "day_id", "policy_id", "policy_code", "curr_insured", "getNull() as charged_premium", "getNow() as create_time", "getNow() as update_time")
    result

  }

  /**
    * spark配置信息以及上下文
    * @param appName 名称
    * @param exceType 执行类型，本地/集群
    */
  def sparkConfInfo(appName:String,exceType:String): (SparkConf, SparkContext, SQLContext, HiveContext) = {
    val conf = new SparkConf()
      .setAppName(appName)
    if (exceType != "") {
      conf.setMaster(exceType)
    }

    val sc = new SparkContext(conf)

    val sqlContest = new SQLContext(sc)

    val hiveContext = new HiveContext(sc)

    (conf, sc, sqlContest, hiveContext)

  }

  /**
    * 获取 Mysql 表的数据
    *
    * @param sqlContext
    * @param tableName 读取Mysql表的名字
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlTable(sqlContext: SQLContext, tableName: String): DataFrame = {

    sqlContext
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://172.16.11.105:3306/odsdb?tinyInt1isBit=false&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "odsuser")
      .option("password", "odsuser")
      .option("numPartitions","10")
      .option("partitionColumn","id")
      .option("lowerBound", "0")
      .option("upperBound","200")
      .option("dbtable", tableName)
      .load()

  }

  /**
    * 将dataframe保存成文件
    * @param path 路径
    * @param tep_end dataframe
    */
  //删除hdfs的文件，后输出
  def delete(path: String, tep_end: RDD[String]): Unit = {
    val properties = getProPerties()
    val hdfsUrl = properties.getProperty("hdfs_url")
    val output = new org.apache.hadoop.fs.Path(hdfsUrl + path)
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(hdfsUrl), new org.apache.hadoop.conf.Configuration())
    // 删除输出目录
    if (hdfs.exists(output)) {
      hdfs.delete(output, true)
      println("delete!--" + hdfsUrl + path)
      tep_end.repartition(1).saveAsTextFile(path)
    } else tep_end.repartition(1).saveAsTextFile(path)
  }

  /**
    * 获取配置文件
    * @return
    */
  def getProPerties() = {
    val lines_source = Source.fromURL(getClass.getResource("/config-scala.properties")).getLines.toSeq
    var properties: Properties = new Properties()
    for (elem <- lines_source) {
      val split = elem.split("==")
      val key = split(0)
      val value = split(1)
      properties.setProperty(key,value)
    }
    properties
  }

}
