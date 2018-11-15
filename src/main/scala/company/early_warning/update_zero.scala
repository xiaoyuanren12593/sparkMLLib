package company.early_warning

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by MK on 2018/8/13.
  */
object update_zero {


  def main(args: Array[String]): Unit = {
    val lines: Iterator[String] = Source.fromURL(getClass.getResource("/config_scala.properties")).getLines
    val url: String = lines.filter(_.contains("location_mysql_url")).map(_.split("==")(1)).mkString("")
    val conf_spark: SparkConf = new SparkConf().setAppName("piwik").set("spark.sql.broadcastTimeout", "36000").setMaster("local[4]")
    val sc = new SparkContext(conf_spark)
    val sqlContext: HiveContext = new HiveContext(sc)

    val prop: Properties = new Properties
    val mid_guzhu_ent_label = sqlContext.read.jdbc(url, "mid_guzhu_warning_detail", prop)

    val columns = mid_guzhu_ent_label.columns :+ "new_content"

    val data_type = mid_guzhu_ent_label.schema.map(_.dataType) :+ StringType
    val before_end = columns.zip(data_type)


    val schema = StructType(before_end.map(x => StructField(x._1, x._2, nullable = true)))
    val row_data = mid_guzhu_ent_label.map(x => {

      val content = x.getAs[String]("content")
      val content_json = JSON.parseObject(content)
      val ringRatio = content_json.getString("ringRatio")

      val new_ringRatio = if (ringRatio == "Infinity") "0.0000" else ringRatio

      content_json.put("ringRatio", new_ringRatio)
      x.toSeq :+ s"$content_json"
    })
    val value = row_data.map(r => Row(r: _*))

    val tep_one = sqlContext.createDataFrame(value, schema)
    val new_content = tep_one.col("new_content")
    tep_one.withColumn("content", new_content).drop("new_content").map(_.mkString("mk6")).repartition(1).saveAsTextFile("C:\\Users\\a2589\\Desktop\\需求one\\earle")
  }
}
