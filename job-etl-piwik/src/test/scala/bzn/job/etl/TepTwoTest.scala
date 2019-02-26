package bzn.job.etl

import java.io.File
import java.sql.DriverManager

import bzn.job.until.PiwikUntil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by MK on 2018/7/12.
  */
object TepTwoTest extends PiwikUntil {

  def loadToHive(sqlContext: HiveContext): DataFrame = {
    //得到piwik_log_action可找到type
    val piwik_log_action = sqlContext.sql("select * from piwik_vt.piwik_log_action")
      .map(x => {
        (x.getAs[String]("idaction"), x.getAs[String]("name"))
      })
      .collectAsMap()

    val piwik_idcation = sqlContext.sql("select *  from piwik_vt.piwik_idcation")
    val big_tepOne = piwik_idcation.map(x => {
      val idaction_event_category = x.getAs[String]("idaction_event_category")
      val idaction_name = x.getAs[String]("idaction_name")
      val idaction_event_action = x.getAs[String]("idaction_event_action")

      val idaction_event_category_name = piwik_log_action.getOrElse(idaction_event_category, "0")
      val idaction_name_vt = piwik_log_action.getOrElse(idaction_name, "0")
      val idaction_event_action_name = piwik_log_action.getOrElse(idaction_event_action, "0")
      Array(idaction_event_category_name, idaction_name_vt, idaction_event_action_name) ++ x.toSeq
    })

    val fields = Array("idaction_event_category_name", "idaction_name_vt", "idaction_event_action_name") ++ piwik_idcation.schema.fieldNames
    //取得列名,并将其作为字段名
    val schema_tepOne = StructType(fields.map(fieldName => StructField(fieldName, StringType, nullable = true)))
    //字段对应的值
    val value_tepTwo = big_tepOne.map(r => Row(r: _*))
    //大表生成DF
    val vt = sqlContext.createDataFrame(value_tepTwo, schema_tepOne)
      .withColumnRenamed("idsite", "idsite_vt")
      .withColumnRenamed("idvisitor", "idvisitor_vt")
      .withColumnRenamed("idvisit", "idvisit_vt")
      .persist(StorageLevel.MEMORY_AND_DISK)

    //与访问表的idvisit进行join从而找到访问表的各个数据
    val piwik_log_visit = sqlContext.sql("select *  from piwik_vt.piwik_log_visit")
    val end = piwik_log_visit.join(vt, piwik_log_visit("idvisit") === vt("idvisit_vt"))

    val end_fields = Array(
      "idvisit",
      "idvisitor",
      "visit_entry_idaction_url",
      "visit_entry_idaction_url_name",
      "visit_exit_idaction_url",
      "visit_exit_idaction_url_name",
      "idaction_event_category",
      "idaction_name",
      "idaction_event_action",
      "idaction_event_category_name",
      "idaction_name_vt",
      "idaction_event_action_name",
      "referer_keyword",
      "referer_name",
      "referer_type",
      "visit_last_action_time",
      "visit_first_action_time",
      "visitor_localtime",
      "name"
    )

    import sqlContext.implicits._
    val fields_name_value: DataFrame = end.map(x => {
      val idvisit = x.getAs[String]("idvisit")
      val idvisitor = x.getAs[String]("idvisitor")

      val visit_entry_idaction_url = x.getAs[String]("visit_entry_idaction_url")
      val visit_exit_idaction_url = x.getAs[String]("visit_exit_idaction_url")

      val visit_entry_idaction_url_name = piwik_log_action.getOrElse(visit_entry_idaction_url, "0")
      val visit_exit_idaction_url_name = piwik_log_action.getOrElse(visit_exit_idaction_url, "0")

      val idaction_event_category = x.getAs[String]("idaction_event_category")
      val idaction_name = x.getAs[String]("idaction_name")
      val idaction_event_action = x.getAs[String]("idaction_event_action")

      val idaction_event_category_name = x.getAs[String]("idaction_event_category_name")
      val idaction_name_vt = x.getAs[String]("idaction_name_vt")
      val idaction_event_action_name = x.getAs[String]("idaction_event_action_name")

      val referer_keyword = x.getAs[String]("referer_keyword")
      val referer_name = x.getAs[String]("referer_name")
      val referer_type = x.getAs[String]("referer_type")

      val visit_last_action_time = x.getAs[String]("visit_last_action_time")
      val visit_first_action_time = x.getAs[String]("visit_first_action_time")
      val visitor_localtime = x.getAs[String]("visitor_localtime")
      val name = x.getAs[String]("name")

      (idvisit, idvisitor, visit_entry_idaction_url, visit_entry_idaction_url_name,
        visit_exit_idaction_url, visit_exit_idaction_url_name, idaction_event_category,
        idaction_name, idaction_event_action, idaction_event_category_name, idaction_name_vt,
        idaction_event_action_name, referer_keyword, referer_name, referer_type, visit_last_action_time,
        visit_first_action_time, visitor_localtime, name)
    }).toDF(end_fields: _*)

//    fields_name_value.insertInto(s"piwik_vt.piwik_all_idcation", overwrite = true)
    fields_name_value
  }

  //遍历某目录下所有的文件和子文件
  def subDir(dir: File): Iterator[File] = {
    val dirs = dir.listFiles().filter(_.isDirectory())
    val files = dir.listFiles().filter(_.isFile())
    files.toIterator ++ dirs.toIterator.flatMap(subDir)
  }

  def getC3p0DateSource(path: String, table_name: String, url: String): Boolean = {
    Class.forName("com.mysql.jdbc.Driver")
    //获取连接//http://baidu.com
    val connection = DriverManager.getConnection(url)
    //通过连接创建statement
    var statement = connection.createStatement()
    val sql1 = s"truncate table odsdb.$table_name"
    val sql2 = s"load data infile '$path'  into table odsdb.$table_name fields terminated by 'mk6'"
    statement = connection.createStatement()
    //先删除数据，在导入数据
    statement.execute(sql1)
    statement.execute(sql2)
  }

  //toMysql
  def toMsql(bzn_year: RDD[String], path_hdfs: String, path: String, table_name: String, url: String): Unit = {
    bzn_year.repartition(1).saveAsTextFile(path_hdfs)
    //得到我目录中的该文件
    val res_file = for (d <- subDir(new File(path))) yield if (d.getName.contains("-") && !d.getName.contains(".")) d.getName else "null"

    //得到part-0000
    val end = res_file.filter(_ != "null").mkString("")
    //通过load,将数据加载到MySQL中 : /share/ods_policy_insured_charged_vt/part-0000
    val tep_end = path + "/" + end
    getC3p0DateSource(tep_end, table_name, url)
  }

  def main(args: Array[String]): Unit = {
    val lines: Iterator[String] = Source.fromURL(getClass.getResource("/config_scala.properties")).getLines
    val url_location: String = lines.filter(_.contains("location_mysql_url")).map(_.split("==")(1)).mkString("")

    val conf_spark: SparkConf = new SparkConf().setAppName("piwik").set("spark.sql.broadcastTimeout", "36000")
          .setMaster("local[4]")
    val sc = new SparkContext(conf_spark)
    val sqlContext: HiveContext = new HiveContext(sc)

    val df = loadToHive(sqlContext)
    //存入哪张表中
    val table_name = "ods_piwik_all_idcation"
    //存入mysql
    val tep_end = df.map(_.mkString("mk6"))
    tep_end.take(10).foreach(x => println(x))
    //得到时间戳
    val timeMillions = System.currentTimeMillis
    //HDFS需要传的路径
    val path_hdfs = s"file:///share/${table_name}_$timeMillions"
    //本地需要传的路径
    val path = s"/share/${table_name}_$timeMillions"
    //每天新创建一个目录，将数据写入到新目录中
//    toMsql(tep_end, path_hdfs, path, table_name, url_location)
  }
}
