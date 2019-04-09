package com.bzn.cLabel

import java.util.Properties

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object CLabelTest {

  def main(args: Array[String]): Unit = {
    //得到标签数据
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf_spark = new SparkConf().setAppName(getClass.getName)
    conf_spark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf_spark.set("spark.sql.broadcastTimeout", "36000")
              .setMaster("local[4]")

    val sc: SparkContext = new SparkContext(conf_spark)
    val sqlContext: HiveContext = new HiveContext(sc)
    //读取渠道表
    val lines_source = Source.fromURL(getClass.getResource("/config_scala.properties")).getLines.toSeq
    val prop: Properties = new Properties

    //从测试库中读取手机号信息表
    val tableName = "t_mobile_location"
    val mobilelocation = readMysqlTable(sqlContext,tableName)


    mobilelocation.show(100)
//    //写入hive
//    //    res.insertInto("odsdb_prd.employer_interface_ofo_c",overwrite = true)
//
//    val path = s"/share/ods/employer_interface_ofo_c"
//
//    res.map(x => x.mkString("\\u0001")).repartition(1).saveAsTextFile(path)
    //    val table = "odsdb_prd.employer_interface_ofo_c"
    //    val output_tmp_dir = ""
    //
    //    sqlContext.sql(s"""load data inpath '$output_tmp_dir' overwrite into table $table """)
    //关闭上下文
    sc.stop()
  }






  /**
    * 获取 Mysql 表的数据
    *
    * @param sqlContext
    * @param tableName 读取Mysql表的名字
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlTable(sqlContext: SQLContext, tableName: String): DataFrame = {
    val properties: Properties = getProPerties()
    sqlContext
      .read
      .format("jdbc")
      .option("url", properties.getProperty("mysql_test.url"))
      .option("driver", properties.getProperty("mysql.driver"))
      .option("user", properties.getProperty("mysql.username"))
      .option("password", properties.getProperty("mysql_test.password"))
      //        .option("dbtable", tableName.toUpperCase)
      .option("dbtable", tableName)
      .load()

  }

  /**
    * 获取配置文件
    *
    * @return
    */
  def getProPerties() = {
    //第一种放在集群上找不到config_scala.properties文件，使用第二种。
    //    val properties: Properties = new Properties()
    //    val loader = getClass.getClassLoader
    //    properties.load(new FileInputStream(loader.getResource(proPath).getFile()))
    //    properties
    val lines_source = Source.fromURL(getClass.getResource("/config_scala.properties")).getLines.toSeq
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
