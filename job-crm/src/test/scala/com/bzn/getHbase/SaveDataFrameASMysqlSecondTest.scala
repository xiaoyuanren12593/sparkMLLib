package com.bzn.getHbase

import java.io.FileInputStream
import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * @author 邢万成
  * 2019/13/9
  *
  */
object SaveDataFrameASMysqlSecondTest {
  def main(args: Array[String]): Unit = {
    var hdfsPath: String = ""
    var proPath: String = ""
    var DATE: String = ""

    val sparkConf: SparkConf = new SparkConf().setAppName(getClass.getSimpleName)
      .setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)
    val sqlContext: SQLContext = new HiveContext(sc)

    import sqlContext.implicits._

    //不过滤读取
    val crmCustomFieldsOptions: DataFrame = readMysqlTable(sqlContext, "t_crm_customfields_options")
      .toDF("id","type","sys_field_name","field_id","one_key","one_value")
      .map(x => {
        val id = x.getAs[Long]("id")
        val sys_field_name = x.getAs[String]("sys_field_name")
        val one_key = x.getAs[String]("one_key")
        val one_value = x.getAs[String]("one_value")
        val fieldAndKey =sys_field_name +"_"+ one_key
        (id,fieldAndKey,one_value)
      }).toDF("id","fieldAndKey","one_value").cache()
    crmCustomFieldsOptions.take(10).foreach(println)
    //读取hbase数据
    val hbaseCrmCum: RDD[(ImmutableBytesWritable, Result)] = getHbaseValue(sc)
    val getHbaseKeyValue = hbaseCrmCum.map(tuple => tuple._2).map(result => {
      val key = Bytes.toString(result.getRow)
      //客户名称
      val name = Bytes.toString(result.getValue("baseInfo".getBytes, "name".getBytes))
      //最后所有人
      val lastMasterUserId = Bytes.toString(result.getValue("baseInfo".getBytes, "lastMasterUserId".getBytes))
      //获客渠道
      val getCumChannel = "CustomField_4795_"+Bytes.toString(result.getValue("customField".getBytes, "CustomField_4795".getBytes))
      //客户规模人数
      val CusLevelCount = Bytes.toString(result.getValue("customField".getBytes, "CustomField_4836".getBytes))
      //具体来源
      val CusSpecificFrom = "CustomField_5118_"+Bytes.toString(result.getValue("customField".getBytes, "CustomField_5118".getBytes))
      //客户来源
      val CusFrom = "CustomField_4824_"+Bytes.toString(result.getValue("customField".getBytes, "CustomField_4824".getBytes))

      (key, name,lastMasterUserId,getCumChannel,CusLevelCount,CusSpecificFrom,CusFrom)
    })
      .toDF("key", "name","lastMasterUserId","getCumChannel","CusLevelCount","CusSpecificFrom","CusFrom")
//    getHbaseKeyValue.map(x=> x.toString()).take(10).foreach(println)
//    println(getHbaseKeyValue.count())
    val getCumChannelTmp = getHbaseKeyValue.join(crmCustomFieldsOptions,getHbaseKeyValue("getCumChannel")===crmCustomFieldsOptions("fieldAndKey"),"left")
        .map(x => {
          val key = x.getAs[String]("key")
          val name = x.getAs[String]("name")
          val lastMasterUserId = x.getAs[String]("lastMasterUserId")
          val getCumChannel = x.getAs[String]("one_value")
          val CusLevelCount = x.getAs[String]("CusLevelCount")
          val CusSpecificFrom = x.getAs[String]("CusSpecificFrom")
          val CusFrom = x.getAs[String]("CusFrom")
          (key, name,lastMasterUserId,getCumChannel,CusLevelCount,CusSpecificFrom,CusFrom)
        })
      .toDF("key", "name","lastMasterUserId","getCumChannel","CusLevelCount","CusSpecificFrom","CusFrom")
//    getCumChannelTmp.map(x=> x.toString()).take(10).foreach(println)

    val CusSpecificFromTmp = getCumChannelTmp.join(crmCustomFieldsOptions,getCumChannelTmp("CusSpecificFrom")===crmCustomFieldsOptions("fieldAndKey"),"left")
      .map(x => {
        val key = x.getAs[String]("key")
        val name = x.getAs[String]("name")
        val lastMasterUserId = x.getAs[String]("lastMasterUserId")
        val getCumChannel = x.getAs[String]("getCumChannel")
        val CusLevelCount = x.getAs[String]("CusLevelCount")
        val CusSpecificFrom = x.getAs[String]("one_value")
        val CusFrom = x.getAs[String]("CusFrom")
        (key, name,lastMasterUserId,getCumChannel,CusLevelCount,CusSpecificFrom,CusFrom)
      })
      .toDF("key", "name","lastMasterUserId","getCumChannel","CusLevelCount","CusSpecificFrom","CusFrom")

    val CusFromTmp = CusSpecificFromTmp.join(crmCustomFieldsOptions,CusSpecificFromTmp("CusFrom")===crmCustomFieldsOptions("fieldAndKey"),"left")
      .map(x => {
        val key = x.getAs[String]("key").toInt
        val name = x.getAs[String]("name")
        val lastMasterUserId = x.getAs[String]("lastMasterUserId")
        val getCumChannel = x.getAs[String]("getCumChannel")
        val CusLevelCount = x.getAs[String]("CusLevelCount")
        val CusSpecificFrom = x.getAs[String]("CusSpecificFrom")
        val CusFrom = x.getAs[String]("one_value")
        (key, name,lastMasterUserId,getCumChannel,CusLevelCount,CusSpecificFrom,CusFrom)
      })
      .toDF("id", "name","lastMasterUserId","getCumChannel","CusLevelCount","CusSpecificFrom","CusFrom")

//    CusFromTmp.take(1000).foreach(println)

    //写入mysql
    saveASMysqlTable(CusFromTmp, "crm_field_value", SaveMode.Overwrite, proPath)
  }

  //得到企业标签数据
  def getHbaseValue(sc: SparkContext): RDD[(ImmutableBytesWritable, Result)] = {
    //定义HBase的配置
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "172.16.11.106")

    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "crm_customer")

    val usersRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    )
    usersRDD
  }

    /**
    * 将DataFrame保存为Mysql表
    *
    * @param dataFrame 需要保存的dataFrame
    * @param tableName 保存的mysql 表名
    * @param saveMode  保存的模式 ：Append、Overwrite、ErrorIfExists、Ignore
    * @param proPath   配置文件的路径
    */
  def saveASMysqlTable(dataFrame: DataFrame, tableName: String, saveMode: SaveMode, proPath: String) = {
    var table = tableName
    val properties: Properties = getProPerties()
    val prop = new Properties //配置文件中的key 与 spark 中的 key 不同 所以 创建prop 按照spark 的格式 进行配置数据库
    prop.setProperty("user", properties.getProperty("mysql.username"))
    prop.setProperty("password", properties.getProperty("mysql.password"))
    prop.setProperty("driver", properties.getProperty("mysql.driver"))
    prop.setProperty("url", properties.getProperty("mysql.url"))
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
        conn.close()
      }
      catch {
        case e: Exception =>
          println("MySQL Error:")
          e.printStackTrace()
      }
    }
    dataFrame.write.mode(SaveMode.Append).jdbc(prop.getProperty("url"), table, prop)
  }

  /**
    * 获取 Mysql 表的数据
    *
    * @param sqlContext
    * @param tableName 读取Mysql表的名字
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlTable(sqlContext: SQLContext, tableName: String) = {
    val properties: Properties = getProPerties()
    sqlContext
      .read
      .format("jdbc")
      .option("url", properties.getProperty("mysql.url"))
      .option("driver", properties.getProperty("mysql.driver"))
      .option("user", properties.getProperty("mysql.username"))
      .option("password", properties.getProperty("mysql.password"))
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