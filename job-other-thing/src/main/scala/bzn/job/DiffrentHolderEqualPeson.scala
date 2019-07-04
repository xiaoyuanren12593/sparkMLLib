package bzn.job

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * author:xiaoYuanRen
  * Date:2019/5/22
  * Time:10:19
  * describe: this is new class
  **/
object DiffrentHolderEqualPeson {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName(DiffrentHolderEqualPeson.getClass.getName)
//      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val sQLContext = new HiveContext(sc)

    import sQLContext.implicits._

    val properties = getProPerties()
    val url = properties.getProperty("location_mysql_url_dwdb")
    val prop = new Properties()

    /**
      * 读取产品表
      */
    val dimpProduct = sQLContext.read.jdbc(url,"dim_product",prop)
      .selectExpr("product_code","dim_1")
      .where("dim_1 in ('外包雇主','骑士保','大货车','零工保')")
    //    dimpProduct.show()

    /**
      * 读取保全表
      */
    val odsPolicyDetail = readMysqlTable(sQLContext,"ods_policy_detail")
      .selectExpr("policy_status","policy_id","insure_code")
      .where("policy_status in ('0','1')")
    //    odsPolicyDetail.show()
    /**
      * 读取投保信息表
      */
    val odrHolderCompanyTemp = readMysqlTable(sQLContext,"odr_policy_holder")
      .selectExpr("policy_id","name")
      .where("policy_id is not null")
    //    odrHolderCompany.show()

    /**
      * 得到雇主保单号和投保公司
      */
    val odsPolicyDetailTemp = odsPolicyDetail.join(dimpProduct,odsPolicyDetail("insure_code") === dimpProduct("product_code"))
      .selectExpr("policy_id as policy_no")

    val odrHolderCompany = odsPolicyDetailTemp.join(odrHolderCompanyTemp,odsPolicyDetailTemp("policy_no") === odrHolderCompanyTemp("policy_id"))
      .selectExpr("policy_id","name")
//    odrHolderCompany.show()
    /**
      *读取被保人信息表
      */
    val odsPolicInsuredDetail = readMysqlTable(sQLContext,"ods_policy_insured_detail")
      .selectExpr("policy_id as insured_policy_id","insured_cert_no")
      .where("insured_policy_id is not null")
//    odsPolicInsuredDetail.show()

    /**
      * 投保人和被保人通过保单id进行关联
      */
    val holderInsuredPerson = odrHolderCompany.join(odsPolicInsuredDetail,odrHolderCompany("policy_id") ===odsPolicInsuredDetail("insured_policy_id"))
      .selectExpr("name","insured_cert_no")
      .map(x => {
        val name = x.getAs[String]("name")
        val insured_cert_no = x.getAs[String]("insured_cert_no")
        (name,insured_cert_no)
      })
      .filter(x => x._1 != null)
      .groupByKey()
      .map(x =>{
        val key = x._1
        val value = x._2.toSeq.distinct
        (1,(key,value))
      })
    val innerJoin = holderInsuredPerson.join(holderInsuredPerson)
      .map(x => {
        val default = x._1
        val value = x._2
        val value1: (String, Seq[String]) = x._2._1
        val value2: (String, Seq[String]) = x._2._2

        val holder1 = value1._1
        val holder2 = value2._1
//        val equalDate = value1._2.intersect(value2._2).mkString(" ")
        val equalCount = value1._2.intersect(value2._2).length
        (holder1,holder2,equalCount)
      })
      .filter(x => x._1 !=x._2)
      .toDF("holder1","holder2","equalCount")

    /**
      * 将holder1 和 holder2  拼接成字符串 并转化成数组然后排序，最后在拼接成字符串返回  ，过滤掉重复的数据（）
      */
    val distinctData = innerJoin.map(x => {
      var str = new StringBuilder
      val holder1 = x.getAs[String]("holder1")
      val holder2 = x.getAs[String]("holder2")
      val equalCount = x.getAs[Int]("equalCount")
      val holderTemp = holder1+"\001"+holder2
      val holder = holderTemp.split("\001").toList.sorted.mkString("\001")
      (holder,equalCount)
    }).distinct()

    val res = distinctData.map(x => {
      val split = x._1.split("\001")
      (split(0),split(1),x._2)
    })
      .toDF("holder1","holder2","equalCount")
//    val outputTmpDir = "/share/DiffrentHolderEqualPeson"
//    val output = "odsdb_prd.DiffrentHolderEqualPeson"
//
//    innerJoin.map(x => x.mkString("\001")).repartition(1).saveAsTextFile(outputTmpDir)
//    sQLContext.sql(s"""load data  inpath '$outputTmpDir' overwrite into table $output""")
//
//    innerJoin.take(100).foreach(println)
    saveASMysqlTable(res,"DiffrentHolderEqualPerson",SaveMode.Overwrite)
//    innerJoin.insertInto("odsdb_prd.DiffrentHolderEqualPeson",overwrite = true)

//    innerJoin.write.mode(SaveMode.Overwrite).saveAsTable("odsdb_prd.DiffrentHolderEqualPeson")
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
  def readMysqlTable(sqlContext: SQLContext, tableName: String): DataFrame = {
    val properties: Properties = getProPerties()
    sqlContext
      .read
      .format("jdbc")
      .option("url", properties.getProperty("mysql.url"))
      .option("driver", properties.getProperty("mysql.driver"))
      .option("user", properties.getProperty("mysql.username"))
      .option("password", properties.getProperty("mysql.password"))
      .option("numPartitions","10")
      .option("partitionColumn","id")
      .option("lowerBound", "0")
      .option("upperBound","200")
      .option("dbtable", tableName)
      .load()
  }

  /**
    * 获取配置文件
    *
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
