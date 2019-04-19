package com.bzn.cLable

import java.util.Properties

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object MobileInfoDistinct {

  def main(args: Array[String]): Unit = {
    //得到标签数据
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf_spark = new SparkConf().setAppName(getClass.getName)
    conf_spark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf_spark.set("spark.sql.broadcastTimeout", "36000")
//              .setMaster("local[4]")

    val sc: SparkContext = new SparkContext(conf_spark)
    val sqlContext: HiveContext = new HiveContext(sc)

    //从测试库中读取手机号信息表
//    val tableName = "t_mobile_location"
//    val mobilelocation = readMysqlTable(sqlContext,tableName)

    //从hive中读取手机号信息表
    val mobilelocation = sqlContext.sql("select * from odsdb_prd.t_mobile_location")
    //没有识别的手机号分离
//    splitMobile(mobilelocation:DataFrame)

    //有效手机号过滤,并存入到hive中
    availMobile(mobilelocation:DataFrame,sqlContext)

    //关闭上下文
    sc.stop()
  }

  /**
    * 有效手机号过滤
    * @param mobilelocation
    */
  def availMobile (mobilelocation:DataFrame,sqlContext: SQLContext) = {
    import sqlContext.implicits._
    val resMobile = mobilelocation

    val res = resMobile.map(x => {
      val id = x.getAs[String]("id")
      val mobile = x.getAs[String]("mobile")
      val data_source = x.getAs[String]("data_source")
      val typeMobile = x.getAs[String]("type")
      val province = x.getAs[String]("province")
      val city = x.getAs[String]("city")
      val operator = x.getAs[String]("operator")
      val location = x.getAs[String]("location")
      val create_time_temp = x.getAs[String]("create_time")
      val create_time = create_time_temp.substring(0,create_time_temp.length-2)
      (mobile,(id,typeMobile,city,province,data_source,operator,location,create_time))
    })
      .reduceByKey((x1,x2) =>{
        x1
      })
      .map(x => {
        (x._2._1,x._1,x._2._2,x._2._3,x._2._4,x._2._5,x._2._6,x._2._7,x._2._8)
      })
      .toDF("id","mobile","type","city","province","data_source","operator","location","create_time")

    val path = s"/share/t_mobile_location"  //写入hdfs上的地址
    val table = "odsdb_prd.t_mobile_location"  //hive表

    //结果集写入到hive
    writeHive(sqlContext,res,path,table)

  }

  /**
    * 数据集写入到hive
    * @param sqlContext
    * @param dataFrame
    */
  def writeHive(sqlContext:SQLContext,dataFrame: DataFrame,path:String,table:String) ={
    dataFrame.map(x => x.mkString("\001")).repartition(1).saveAsTextFile(path)
//    sqlContext.sql(s"""load data inpath '$path' overwrite into table $table """)
  }
  /**
    * 没有事别的手机号分离
    * @param mobilelocation
    */
  def splitMobile(mobilelocation:DataFrame) = {
    val mobileNotData = mobilelocation.where("province is null").select("mobile")
      .distinct()
    val path = s"/share/emp_inter_ofo_info_not_pipei"
    mobileNotData.map(x => x.mkString("")).repartition(5).saveAsTextFile(path)
    //    mobilelocation.show(100)
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
      .option("numPartitions","30")
      .option("partitionColumn","id")
      .option("lowerBound", "0")
      .option("upperBound","20000")
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
