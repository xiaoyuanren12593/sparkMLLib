package bzn.job.etl

import java.sql.{Connection, DriverManager}
import java.util.Properties

import bzn.job.common.Until
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

import scala.io.Source

/**
  * author:xiaoYuanRen
  * Date:2019/7/22
  * Time:11:53
  * describe: 投保人（企业）每日在保人数据
  **/
object EntIdCurrPersonTest extends Until{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    val res = getEntIdCurrPersonDetail(hiveContext)
//    saveASMysqlTable(res,"ods_policy_ent_curr_insured",SaveMode.Overwrite)
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
    prop.setProperty("user", properties.getProperty("mysql.username.105"))
    prop.setProperty("password", properties.getProperty("mysql.password.105"))
    prop.setProperty("driver", properties.getProperty("mysql.driver"))
    prop.setProperty("url", properties.getProperty("mysql.url.105"))
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
    * 获取投保人（企业）当前在保人数
    * @param sqlContext
    */
  def getEntIdCurrPersonDetail(sqlContext:HiveContext) ={
    import sqlContext.implicits._
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    /**
      * 读取投保人数据
      */
    val odsPolicyDetail = readMysqlTable(sqlContext,"ods_policy_detail")
        .selectExpr("policy_id","policy_status","ent_id")
      .where("policy_status in ('1','0') and length(ent_id) > 0")

    /**
      * 读取每日在保人明细数据
      */
    val odsPolicyCurrInsured = sqlContext.sql("select policy_id as policy_id_slave,curr_insured,day_id from odsdb_prd.ods_policy_curr_insured")

    val odsPolicy = odsPolicyDetail.join(odsPolicyCurrInsured,odsPolicyDetail("policy_id")===odsPolicyCurrInsured("policy_id_slave"))
      .map(x => {
        val dayId = x.getAs[String]("day_id")
        val entId = x.getAs[String]("ent_id")
        var currInsured = x.getAs[Int]("curr_insured")
        if(currInsured == null){
          currInsured = 0
        }
        ((entId,dayId),currInsured)
      })
      .reduceByKey(_+_)
      .map(x => {
        (x._1._1,x._1._2,x._2)
      })
      .toDF("ent_id","day_id","curr_insured")
      .selectExpr("getUUID() as id","ent_id","day_id","curr_insured")
    odsPolicy.printSchema()
    odsPolicy
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
//      .option("numPartitions","10")
//      .option("partitionColumn","id")
//      .option("lowerBound", "0")
//      .option("upperBound","200")
      .option("dbtable", tableName)
      .load()
  }

  /**
    * 获取配置文件
    *
    * @return
    */
  def getProPerties() = {
    val lines_source = Source.fromURL(getClass.getResource("/config-scala.properties")).getLines.toSeq
    var properties: Properties = new Properties()
    for (elem <- lines_source) {
      val split = elem.split("==")
      val key = split(0)
      val value = split(1)
      properties.setProperty(key, value)
    }
    properties
  }
}
