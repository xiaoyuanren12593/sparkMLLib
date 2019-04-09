package com.bzn.cLabel

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object EmpInterOfoInfoTest {
  def main(args: Array[String]): Unit = {
    //得到标签数据
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf_spark = new SparkConf().setAppName(getClass.getName)
    conf_spark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf_spark.set("spark.sql.broadcastTimeout", "36000")
      .setMaster("local[4]")
    //读取渠道表
    val lines_source = Source.fromURL(getClass.getResource("/config_scala.properties")).getLines.toSeq
    val location_mysql_url_test: String = lines_source(9).toString.split("==")(1)
    val prop: Properties = new Properties

    val sc: SparkContext = new SparkContext(conf_spark)
    val sqlContext: HiveContext = new HiveContext(sc)

    val empInterOfoInfoData = getEmpInterOfoInfoData(sqlContext: HiveContext,location_mysql_url_test,prop: Properties)
    empInterOfoInfoData.show(500)
    println(empInterOfoInfoData.count())
//    saveASMysqlTable(empInterOfoInfoData,"emp_inter_ofo_info_detail",SaveMode.Overwrite)

//    val empInterOfoResData = sqlContext.sql("select * from odsdb_prd.emp_inter_ofo_info_not_pipei")

    //    saveASMysqlTable(empInterOfoInfoData,"emp_inter_ofo_info_detail",SaveMode.Overwrite)
//    val outputTmpDir = "/share/emp_inter_ofo_info_detail"
//    val output = "odsdb_prd.emp_inter_ofo_info_detail"

//    empInterOfoInfoData.rdd.map(x => x.mkString("\001")).repartition(1).saveAsTextFile(outputTmpDir)
//    sqlContext.sql(s"""load data  inpath '$outputTmpDir' overwrite into table $output""")

    sc.stop()
  }

  /**
    * 将DataFrame保存为Mysql表
    *
    * @param dataFrame 需要保存的dataFrame
    * @param tableName 保存的mysql 表名
    * @param saveMode  保存的模式 ：Append、Overwrite、ErrorIfExists、Ignore
    */
  def saveASMysqlTable(dataFrame: DataFrame, tableName: String, saveMode: SaveMode) = {
    var table = tableName
    val properties: Properties = getProPerties()

    val prop = new Properties //配置文件中的key 与 spark 中的 key 不同 所以 创建prop 按照spark 的格式 进行配置数据库
    prop.setProperty("user", properties.getProperty("mysql.username"))
    prop.setProperty("password", properties.getProperty("mysql_test.password"))
    prop.setProperty("driver", properties.getProperty("mysql.driver"))
    prop.setProperty("url", properties.getProperty("mysql_test.url"))
    println(prop.getProperty("url"))
    if (saveMode == SaveMode.Overwrite) {
      var conn: Connection = null
      try {
        conn = DriverManager.getConnection(
          prop.getProperty("url"),
          prop.getProperty("user"),
          prop.getProperty("password")
        )
        val stmt = conn.createStatement
        table = table.toLowerCase
        stmt.execute(s"truncate table $table") //为了不删除表结构，先truncate 再Append
        println("truncate is success")
        conn.close()
      }
      catch {
        case e: Exception =>
          println("MySQL Error:")
          e.printStackTrace()
      }
    }
//    dataFrame.write.mode(SaveMode.Append).jdbc(prop.getProperty("url"), table, prop)
  }


  /**
    * 获取配置文件
    *
    * @return
    */
  def getProPerties(): Properties = {
    //第一种放在集群上找不到config_scala.properties文件，使用第二种。
    //    val properties: Properties = new Properties()
    //    val loader = getClass.getClassLoader
    //    properties.load(new FileInputStream(loader.getResource(proPath).getFile()))
    //    properties
    val lines_source = Source.fromURL(getClass.getResource("/config_scala.properties")).getLines.toSeq
//    lines_source.foreach(println)
    var properties: Properties = new Properties()
    for (elem <- lines_source) {
      val split = elem.split("==")
      val key = split(0)
      val value = split(1)
      properties.setProperty(key,value)
    }
    properties
  }

  /**
    * 得到信息明细表
    * @param sqlContext
    */
  def getEmpInterOfoInfoData(sqlContext: HiveContext,location_mysql_url_test:String,prop: Properties) = {

    import sqlContext.implicits._
    //读取雇主、接口、ofo结果数据
    val empInterOfoResData = sqlContext.read.jdbc(location_mysql_url_test,"emp_inter_ofo_res",prop)
      .select("insured_name","insured_mobile","insured_cert_no","gender","age")
      .map(x=> {
        val insured_name = x.getAs[String]("insured_name")
        val insured_mobile = x.getAs[String]("insured_mobile")
        val insured_cert_no = x.getAs[String]("insured_cert_no")
        val gender = x.getAs[String]("gender")
        val age = x.getAs[String]("age")
        val key = insured_mobile +"\u0001"+insured_cert_no+"\u0001"+gender+"\u0001"+age
        (key,insured_name)
      })
      .reduceByKey((x1,x2) => {
        val names = x1+"\u0001"+x2
        names
      })
      .map(x => {
        val names = x._2.trim.split("\u0001")
        var insured_name = ""
        if(names.length == 1){
          insured_name = names(0)
        }
        val second = x._1.split("\u0001")
        (insured_name,second(0),second(1),second(2),second(3))
      })
      .toDF("insured_name","insured_mobile","insured_cert_no","gender","age")

//    //读取手机号归属地明细表
    val tMobileLocationData = sqlContext.read.jdbc(location_mysql_url_test,"t_mobile_location",prop)
      .select("mobile","city","province","type","operator","location")
//
    //通过手机号join
    val empInterOfoInfoData = tMobileLocationData.join(empInterOfoResData,tMobileLocationData("mobile")===empInterOfoResData("insured_mobile"),"rightouter")
      .select("insured_name","insured_mobile","insured_cert_no","gender","age","province","city","type","operator","location")
    empInterOfoInfoData

  }

}
