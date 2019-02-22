package bzn.job.etl

import java.util.Properties

import bzn.job.until.PiwikUntil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by MK on 2018/7/11.
  * 在SBT构建的项目中， src/main 和 src/test 目录下都有一个名为 resources 的目录，用来存放相应的资源文件。
  * 那么如何来读取相应 resources 目录中的资源文件呢？
  * 现假设有 test.data 文件，存放在 main/resources/data/test.data 中，文件内容为：
  * getResource("/data/test.data"))
  */

object TepOne extends PiwikUntil {

  System.setProperty("HADOOP_USER_NAME", "hdfs")

  //字段与字段值合并
  def merge(df: DataFrame): (RDD[Row], StructType) = {
    //过滤完后剩余19个字段
    val columns: Seq[String] = df.schema.map(x => x.name).filter(!_.contains("custom_var"))
    val length_number = columns.length
    val row_data = df.map(x => x.toSeq.map(x => if (x == null) "null" else x.toString)
      .take(length_number)
      .map(x =>
        if (x.contains(":") && x.contains("-") && number_if_not(x.split("-")(0))) eight_date(x)
        else if (x.contains(":") && number_if_not(x.split(":")(0)) && number_if_not(x.split(":")(1)) && !x.contains("-")) eight_date_only_hour(x)
        else x
      )
    )
    //seq.take(19)提取前19个字段数据
    //取得列名,并将其作为字段名
    val schema = StructType(columns.map(fieldName => StructField(fieldName, StringType, nullable = true)))
    //字段对应的值
    val value = row_data.map(r => Row(r: _*))
    (value, schema)
  }

  //将3张表以parquet的方式存入到hive中
  def load_three_table(prop: Properties, sqlContext: HiveContext, url: String): (DataFrame, DataFrame, DataFrame) = {
    //表生成piwik_log_link_visit_action(DF)表
    val piwik_log_link_visit_action_before: DataFrame = sqlContext.read.jdbc(url, "piwik_log_link_visit_action", prop)
    piwik_log_link_visit_action_before.registerTempTable("a")
    //对二进制字段进行转换
    val link_idvisitor_one = sqlContext.sql("select HEX(idvisitor) as idvisitor_str,* from a")
    val link_idvisitor_two = sqlContext.createDataFrame(merge(link_idvisitor_one)._1, merge(link_idvisitor_one)._2)
    //将字段转换类型保持一致，然后将二进制以后的字段(idvisitor_str)，覆盖住以前的字段(idvisitor),没有的话会重新生成一个新的字段
    val idvisitor_str_link = link_idvisitor_two.selectExpr("idvisitor_str").col("idvisitor_str")
    val one = link_idvisitor_two.withColumn("idvisitor", idvisitor_str_link).drop("idvisitor_str")

    val piwik_log_action = sqlContext.read.jdbc(url, "piwik_log_action", prop)
    val two = sqlContext.createDataFrame(merge(piwik_log_action)._1, merge(piwik_log_action)._2) //.show()

    val piwik_log_visit_before = sqlContext.read.jdbc(url, "piwik_log_visit", prop)
    piwik_log_visit_before.registerTempTable("b")
    val visit_idvisitor_one = sqlContext.sql("select HEX(idvisitor) as idvisitor_str,* from b")
    val visit_idvisitor_two = sqlContext.createDataFrame(merge(visit_idvisitor_one)._1, merge(visit_idvisitor_one)._2)

    val idvisitor_str_visit = visit_idvisitor_two.selectExpr("idvisitor_str").col("idvisitor_str")
    val three = visit_idvisitor_two.withColumn("idvisitor", idvisitor_str_visit).drop("idvisitor_str")

    one.insertInto(s"piwik_vt.piwik_log_link_visit_action", overwrite = true)
    two.insertInto(s"piwik_vt.piwik_log_action", overwrite = true)
    three.insertInto(s"piwik_vt.piwik_log_visit", overwrite = true)
    (one, two, three)
  }

  //过滤官网订单，同时根据idaction过滤一版,并将结果存到hive中
  def laod_idaction_table(three_idcation: (DataFrame, DataFrame, DataFrame), sqlContext: HiveContext): DataFrame = {
    val piwik_log_link_visit_action = three_idcation._1
    val piwik_log_action = three_idcation._2
    val piwik_log_visit = three_idcation._3

    val one = piwik_log_action
      .map(x => {
        val name = to_null(x.getAs[String]("name"))
        val idaction = to_null(x.getAs[String]("idaction"))
        (idaction, (name, x))
      })
      .map(x => (x._1, x._2._2)).persist(StorageLevel.MEMORY_ONLY)

    //通过idcation进行关联
    val field_value = piwik_log_link_visit_action
      .map(x => {
        val idaction_event_category = to_null(x.getAs[String]("idaction_event_category"))
        //idaction,row
        (idaction_event_category, x)
      })
      .join(one)
      .map(x => x._2._1.toSeq ++ x._2._2.toSeq)

    val field_names = piwik_log_link_visit_action.schema.map(_.name) ++ piwik_log_action.schema.map(_.name)

    val row_data: RDD[Seq[String]] = field_value.map(x => x.map(x => if (x == null) "null" else x.toString))
    //取得列名,并将其作为字段名
    val schema = StructType(field_names.map(fieldName => StructField(fieldName, StringType, nullable = true)))
    //字段对应的值
    val value = row_data.map(r => Row(r: _*))
    val piwik_idcation: DataFrame = sqlContext.createDataFrame(value, schema)
    piwik_idcation.insertInto(s"piwik_vt.piwik_idcation", overwrite = true)
    piwik_idcation
  }

  def main(args: Array[String]): Unit = {
    val lines: Iterator[String] = Source.fromURL(getClass.getResource("/config_scala.properties")).getLines
    val url: String = lines.filter(_.contains("cloud_piwik_url")).map(_.split("==")(1)).mkString("")
    val conf_spark: SparkConf = new SparkConf().setAppName("piwik").set("spark.sql.broadcastTimeout", "36000")
    //    .setMaster("local[4]")
    val sc = new SparkContext(conf_spark)
    val sqlContext: HiveContext = new HiveContext(sc)

    val prop: Properties = new Properties
    //将原始数据存到hive对应的表中
    val three_idcation = load_three_table(prop, sqlContext, url)
    //过滤官网订单，同时根据idaction过滤一版,并将结果存到hive中
    laod_idaction_table(three_idcation, sqlContext)
    sc.stop()
  }
}
