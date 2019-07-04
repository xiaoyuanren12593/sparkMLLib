package bzn.job


import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.math.BigDecimal.RoundingMode

object SimilyWordsHolderName {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getName)

    val sc = new SparkContext(sparkConf)

    //    val sQLContext = new SQLContext(sc)

    val hiveContext = new HiveContext(sc)

    //比较识别的保险公司和正确保险公司的相似度
    //    val tOcrVatinvoiceData = similarityMatch(sQLContext,"t_ocr_vatinvoice",properties.getProperty("location_mysql_url_dwdb_1"))

    val ods_policy_detail =
      hiveContext.sql("select * from odsdb_prd.ods_policy_detail")
        .selectExpr("trim (holder_name) as holder_name","insurant_company_name","insure_code")
        .cache()

    val dim_product =
      hiveContext.sql("select * from odsdb_prd.dim_product")
        .where("dim_1 in ('外包雇主','大货车','骑士保','零工保')")
        .selectExpr("product_code")
        .cache()
    //d.dim_1 in ('外包雇主','大货车','骑士保','零工保')
    val policyHolderTemp = ods_policy_detail.join(dim_product,ods_policy_detail("insure_code") === dim_product("product_code"))
      .distinct()
      .cache()

    val oneHolder = policyHolderTemp.selectExpr("1 as join_id","holder_name").unionAll(policyHolderTemp.selectExpr("1 as join_id","insurant_company_name"))
      .toDF("join_id","holder_name")
      .where("length(holder_name) > 1")
      .distinct()
      .where("holder_name is not  null")

    similarityMatch(hiveContext,oneHolder)

    sc.stop()
  }

  /**

    * 数据集写入到hive
    * @param sqlContext
    * @param dataFrame
    */
  def writeHive(sqlContext:HiveContext,dataFrame: DataFrame,path:String,table:String) ={
    dataFrame.map(x => x.mkString("\001")).repartition(1).saveAsTextFile(path)
    sqlContext.sql(s"""load data inpath '$path' overwrite into table $table """)
  }

  /**
    * 比较识别的发票抬头和正确保险公司的相似度
    * @param sQLContext
    * @return
    */
  def similarityMatch(sQLContext:HiveContext,data:DataFrame) ={
    import sQLContext.implicits._
    sQLContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//设置日期格式
      val date = df.format(new Date())// new Date()为获取当前系统时间
      (date + "")
    })
    //发票识别表
    val dw_bm_vatinvoice = readMysqlTable(sQLContext,"dw_bm_vatinvoice")
    //读取发票抬头更正表
    val dw_bm_vatinvoice_updata = readMysqlTable(sQLContext,"dw_bm_vatinvoice_update")
      .selectExpr("id as id_update","purchaser_name ","purchaser_res")

    /**
      * 创建临时数据发票抬头识别数据
      */
    val dw_bm_vatinvoiceTemp = dw_bm_vatinvoice.selectExpr("id","purchaser_name as purchaser_name_temp","1 as temp_join")

    /**
      * 新增的数据
      */
    val dw_bm_vatinvoiceUpdata = dw_bm_vatinvoiceTemp.join(dw_bm_vatinvoice_updata,dw_bm_vatinvoiceTemp("id")===dw_bm_vatinvoice_updata("id_update"),"leftouter")
      .where("id_update is null")
      .selectExpr("id","purchaser_name_temp as purchaser_name","temp_join")

    if(dw_bm_vatinvoiceUpdata.count() > 0){
      /**
        * 新增的数据和所有投保公司进行关联
        */
      val vatinvoicePolicy = dw_bm_vatinvoiceUpdata.join(data,dw_bm_vatinvoiceUpdata("temp_join")===data("join_id"),"leftouter")
        .selectExpr("id","purchaser_name","holder_name")

      val resTemp =  vatinvoicePolicy
        .map(x => {
          val id = x.getAs[Long]("id").toString
          val purchaser_name = x.getAs[String]("purchaser_name")
          val holder_name = x.getAs[String]("holder_name")
          if(purchaser_name != null && purchaser_name.length != 0){
            val r = BigDecimal(textCosine(purchaser_name,holder_name)).setScale(4,RoundingMode.HALF_UP).doubleValue()
            (id,(purchaser_name,holder_name,r))
          }else{
            (id,(purchaser_name,purchaser_name,1.0))
          }
        })
        .reduceByKey((x1,x2) => {
          val d_dis = if(x1._3 > x2._3) x1 else x2
          d_dis
        })
        .map(x => {
          (x._1,x._2._1,x._2._2)
        })
        .filter(x=>{
          if( x._2 != null && x._2.length != 0) true else false
        })
        .toDF("id_slave","purchaser_name","purchaser_res")

      /**
        * 新增字段和已更新字段进行union
        */
      val res = resTemp
        .selectExpr("id_slave as id","purchaser_name","purchaser_res","getNow() as updata_time")
      /**
        * 保存到mysql
        */
      saveASMysqlTable(res,"dw_bm_vatinvoice_update",SaveMode.Append)
    }else{
      println("没有更新数据")
    }
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
    prop.setProperty("url", properties.getProperty("location_mysql_url_dwdb_1"))
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
  def readMysqlTable(sqlContext: HiveContext, tableName: String): DataFrame = {
    val properties: Properties = getProPerties()
    sqlContext
      .read
      .format("jdbc")
      .option("url", properties.getProperty("location_mysql_url_dwdb_1"))
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


  /**
    * 求向量的模
    * @param vec
    * @return
    */
  def module(vec: Vector[Double]) = {
    math.sqrt(vec.map(math.pow(_, 2)).sum)
  }

  /**
    * 求两个向量的内积
    * @param v1
    * @param v2
    * @return
    */
  def innerProduct(v1: Vector[Double], v2: Vector[Double]) =
  {
    val listBuffer = ListBuffer[Double]()
    for (i <- 0 until v1.length; j <- 0 to v2.length; if i == j) {
      if (i == j)
        listBuffer.append(v1(i) * v2(j))
    }
    listBuffer.sum
  }

  /**
    * 求两个向量的余弦
    * @param v1
    * @param v2
    * @return
    */

  def cosvec(v1: Vector[Double], v2: Vector[Double]) = {
    val cos = innerProduct(v1, v2) / (module(v1) * module(v2))
    if (cos <= 1) cos else 1.0
  }

  /**
    * 余弦相似度
    * @param str1
    * @param str2
    * @return
    */
  def textCosine(str1: String, str2: String) = {
    val set = mutable.Set[Char]()
    // 不进行分词
    str1.foreach(set += _)
    str2.foreach(set += _)
    val ints1: Vector[Double] = set.toList.sorted.map(ch => {
      str1.count(s => s == ch).toDouble }).toVector
    val ints2: Vector[Double] = set.toList.sorted.map(ch => {
      str2.count(s => s == ch).toDouble
    }).toVector
    cosvec(ints1, ints2)
  }
}
