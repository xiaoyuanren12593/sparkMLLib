package com.bzn.getHbase

import java.io.FileInputStream
import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.Properties

import com.bzn.util.Spark_Util
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
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.io.Source

/**
  * @author 邢万成
  * 2019/13/9
  *
  */
object SaveDataFrameASMysql {

  def main(args: Array[String]): Unit = {
    var hdfsPath: String = ""
    var proPath: String = ""
    var DATE: String = ""

    val sparkConf: SparkConf = new SparkConf().setAppName(getClass.getSimpleName)
//        .setMaster("local[*]")
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

    //读取hbase数据
    val hbaseCrmCum: RDD[(ImmutableBytesWritable, Result)] = getHbaseValue(sc)
    val getHbaseKeyValue = hbaseCrmCum.map(tuple => tuple._2).map(result => {
      val key = Bytes.toString(result.getRow)
      //客户名称
      val name = Bytes.toString(result.getValue("baseInfo".getBytes, "name".getBytes))
      //客户类型
      val businessCategoryId = Bytes.toString(result.getValue("baseInfo".getBytes, "businessCategoryId".getBytes))
      //创建人
      val createUser = Bytes.toString(result.getValue("baseInfo".getBytes, "createUser".getBytes))
      //客户所有人
      val master = Bytes.toString(result.getValue("baseInfo".getBytes, "master".getBytes))
      //客户所属部门
      val masterOffice = Bytes.toString(result.getValue("baseInfo".getBytes, "masterOffice".getBytes))
      //最后所有人
      val lastMasterUserId = Bytes.toString(result.getValue("baseInfo".getBytes, "lastMasterUserId".getBytes))
      //最近修改时间
      val updateTime = Bytes.toString(result.getValue("baseInfo".getBytes, "updateTime".getBytes))
      //获客渠道
      val getCumChannel = "CustomField_4795_"+Bytes.toString(result.getValue("customField".getBytes, "CustomField_4795".getBytes))
      //客户规模人数
      val CusLevelCount = Bytes.toString(result.getValue("customField".getBytes, "CustomField_4836".getBytes))
      //具体来源
      val CusSpecificFrom = "CustomField_5118_"+Bytes.toString(result.getValue("customField".getBytes, "CustomField_5118".getBytes))
      //客户来源
      val CusFrom = "CustomField_4824_"+Bytes.toString(result.getValue("customField".getBytes, "CustomField_4824".getBytes))

      (key, name,businessCategoryId,createUser,master,masterOffice,lastMasterUserId,updateTime,getCumChannel,CusLevelCount,CusSpecificFrom,CusFrom)
    })
      .toDF("key","name","businessCategoryId","createUser","master","masterOffice","lastMasterUserId","updateTime","getCumChannel","CusLevelCount",
        "CusSpecificFrom","CusFrom")


    val getCumChannelTmp = getHbaseKeyValue.join(crmCustomFieldsOptions,getHbaseKeyValue("getCumChannel")===crmCustomFieldsOptions("fieldAndKey"),"left")
      .map(x => {
        val key = x.getAs[String]("key")
        val name = x.getAs[String]("name")
        val businessCategoryId = x.getAs[String]("businessCategoryId")
        val createUser = x.getAs[String]("createUser")
        val master = x.getAs[String]("master")
        val masterOffice = x.getAs[String]("masterOffice")
        val lastMasterUserId = x.getAs[String]("lastMasterUserId")
        val updateTime = x.getAs[String]("updateTime")
        val getCumChannel = x.getAs[String]("one_value")
        val CusLevelCount = x.getAs[String]("CusLevelCount")
        val CusSpecificFrom = x.getAs[String]("CusSpecificFrom")
        val CusFrom = x.getAs[String]("CusFrom")
        (key, name,businessCategoryId,createUser,master,masterOffice,lastMasterUserId,updateTime,getCumChannel,CusLevelCount,CusSpecificFrom,CusFrom)
      })
      .toDF("key", "name","businessCategoryId","createUser","master","masterOffice", "lastMasterUserId","updateTime","getCumChannel","CusLevelCount","CusSpecificFrom","CusFrom")

    val CusSpecificFromTmp = getCumChannelTmp.join(crmCustomFieldsOptions,getCumChannelTmp("CusSpecificFrom")===crmCustomFieldsOptions("fieldAndKey"),"left")
      .map(x => {
        val key = x.getAs[String]("key")
        val name = x.getAs[String]("name")
        val businessCategoryId = x.getAs[String]("businessCategoryId")
        val createUser = x.getAs[String]("createUser")
        val master = x.getAs[String]("master")
        val masterOffice = x.getAs[String]("masterOffice")
        val lastMasterUserId = x.getAs[String]("lastMasterUserId")
        val updateTime = x.getAs[String]("updateTime")
        val getCumChannel = x.getAs[String]("getCumChannel")
        val CusLevelCount = x.getAs[String]("CusLevelCount")
        val CusSpecificFrom = x.getAs[String]("one_value")
        val CusFrom = x.getAs[String]("CusFrom")
        (key, name,businessCategoryId,createUser,master,masterOffice,lastMasterUserId,updateTime,getCumChannel,CusLevelCount,CusSpecificFrom,CusFrom)
      })
      .toDF("key", "name","businessCategoryId","createUser","master","masterOffice", "lastMasterUserId","updateTime","getCumChannel","CusLevelCount","CusSpecificFrom","CusFrom")

    val CusFromTmp = CusSpecificFromTmp.join(crmCustomFieldsOptions,CusSpecificFromTmp("CusFrom")===crmCustomFieldsOptions("fieldAndKey"),"left")
      .map(x => {
        val key = x.getAs[String]("key").toInt
        val name = x.getAs[String]("name")
        val businessCategoryId = x.getAs[String]("businessCategoryId")
        val createUser = x.getAs[String]("createUser")
        val master = x.getAs[String]("master")
        val masterOffice = x.getAs[String]("masterOffice")
        val lastMasterUserId = x.getAs[String]("lastMasterUserId")
        val updateTime = x.getAs[String]("updateTime")
        val getCumChannel = x.getAs[String]("getCumChannel")
        val CusLevelCount = x.getAs[String]("CusLevelCount")
        val CusSpecificFrom = x.getAs[String]("CusSpecificFrom")
        val CusFrom = x.getAs[String]("one_value")
        (key, name,businessCategoryId,createUser,master,masterOffice,lastMasterUserId,updateTime,getCumChannel,CusLevelCount,CusSpecificFrom,CusFrom)
      })
      .toDF("id", "name","businessCategoryId","createUser","master","masterOffice","lastMasterUserId","updateTime","getCumChannel","CusLevelCount","CusSpecificFrom","CusFrom")

    //得到商机的数据
    val bussValue = getHbaseBussValue(sc)

    val bussValueTemp = bussValue.map(x => x._2).map(x => {
      //rowkey
      val key = Bytes.toString(x.getRow)

      //客户id
      val customerId = Bytes.toString(x.getValue("baseInfo".getBytes, "customerId".getBytes))
      //销售阶段
      val saleProcess = Bytes.toString(x.getValue("baseInfo".getBytes, "saleProcess".getBytes))
      //投保企业全称
      val entName = Bytes.toString(x.getValue("customField".getBytes, "CustomField_12066".getBytes))
      (key,customerId, saleProcess, entName)
    })
      .toDF("key","customerId","saleProcess","entName")
    val res = CusFromTmp.join(bussValueTemp,CusFromTmp("id") === bussValueTemp("customerId"),"leftouter")
      .select("id", "name","businessCategoryId","createUser","master","masterOffice","lastMasterUserId","updateTime","getCumChannel",
        "CusLevelCount","CusSpecificFrom","CusFrom","saleProcess","entName")
      .distinct()
      .cache()
    //crm_field_value  的 name， entName， getCumChannel
    val resAll = res.selectExpr("id", "name","businessCategoryId","createUser","master","masterOffice","lastMasterUserId","getCumChannel",
      "CusLevelCount","CusSpecificFrom","CusFrom","saleProcess","entName")

    /**
      * "name","entName","getCumChannel"  保留唯一一条
      */
    val resTwo = res.selectExpr("name","entName","getCumChannel","CusLevelCount","updateTime")
      .map(x => {
        val name = x.getAs[String]("name")
        val entName = x.getAs[String]("entName")
        val getCumChannel = x.getAs[String]("getCumChannel")
        val cusLevelCount = x.getAs[String]("CusLevelCount")
        val updateTime = x.getAs[String]("updateTime")
        var updateTimeRes = 0L
        if(updateTime != null){
          updateTimeRes = currentTimeL(updateTime)
        }
        var nameRes = ""
        if(entName == null){
          if(name != null){
            nameRes = name.trim
          }else{
            nameRes = null
          }
        }else{
          nameRes = entName.trim
        }
        println(nameRes)
        (nameRes,(getCumChannel,cusLevelCount,updateTimeRes))
      })
      .reduceByKey((x1,x2)=>{
        val res = if(x1._3>x2._3) x1 else x2
        res
      })
      .map(x => (x._1,x._2._1,x._2._2))
      .toDF("ent_name","cum_channel","cus_level_count")

    saveASMysqlTable(resAll, "crm_field_value", SaveMode.Overwrite)
    saveASMysqlTable(resTwo, "crm_cum_channel", SaveMode.Overwrite)
    sc.stop()
  }

  //得到客户数据
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

  //得到商机数据
  def getHbaseBussValue(sc: SparkContext): RDD[(ImmutableBytesWritable, Result)] = {
    //定义HBase的配置
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "172.16.11.106")

    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "crm_niche")

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
    */
  def saveASMysqlTable(dataFrame: DataFrame, tableName: String, saveMode: SaveMode) = {
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

  //将时间转换为时间戳
  def currentTimeL(str: String): Long = {
    val format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

    val insured_create_time_curr_one: DateTime = DateTime.parse(str, format)
    val insured_create_time_curr_Long: Long = insured_create_time_curr_one.getMillis

    insured_create_time_curr_Long
  }

  /**
    * 获取 Mysql 表的数据 添加过滤条件
    *
    * @param sqlContext
    * @param table           读取Mysql表的名字
    * @param filterCondition 过滤条件
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlTable(sqlContext: SQLContext, table: String, filterCondition: String) = {
    val properties: Properties = getProPerties()
    var tableName = ""
    tableName = "(select * from " + table + " where " + filterCondition + " ) as t1"
    sqlContext
      .read
      .format("jdbc")
      .option("url", properties.getProperty("mysql.url"))
      .option("driver", properties.getProperty("mysql.driver"))
      .option("user", properties.getProperty("mysql.username"))
      .option("password", properties.getProperty("mysql.password"))
      .option("dbtable", tableName)
      .load()
  }

  /**
    * 获取配置文件
    * @return
    */
  def getProPerties() = {
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