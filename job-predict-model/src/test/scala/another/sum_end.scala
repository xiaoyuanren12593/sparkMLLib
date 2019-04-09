package another

import java.io.StringReader
import java.util.Properties

import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MK on 2018/5/28.
  */
object sum_end {
  def table(sc: SparkContext, sQLContext: SQLContext, path_big: String): DataFrame
  = {
    //雇主大表
    val big_tepOne = sc.textFile(path_big).map(x => {
      val reader = new CSVReader(new StringReader(x))
      reader.readNext()
    })
    //取得列名,并将其作为字段名
    val schema_tepOne = StructType(big_tepOne.first.map(fieldName => StructField(fieldName, StringType, nullable = true)))
    //字段对应的值
    val value_tepTwo = big_tepOne.map(r => Row(r: _*))
    //大表生成DF
    val big = sQLContext.createDataFrame(value_tepTwo, schema_tepOne)
    big
  }

  def main(args: Array[String]): Unit = {
    val conf_s = new SparkConf().setAppName("wuYu").setMaster("local[2]")
    val sc = new SparkContext(conf_s)
    val sQLContext: SQLContext = new SQLContext(sc)
    val end = "C:\\Users\\a2589\\Desktop\\ent_tep.csv"
    import sQLContext.implicits._

    val end_table = table(sc, sQLContext, end).filter("length(ent_id)>13")
   val end_tep =  end_table.map(x => {
      val ent_id = x.getAs[String]("ent_id")
      val cert = x.getAs("cert").toString

      val banan = x.getAs[String]("banan_value").toInt
      val person_risk = x.getAs[String]("person_risk").toInt
      val enter_value = x.getAs[String]("enter_value").toDouble
      val enter_risk = x.getAs[String]("enter_risk").toInt

      val one_one = if (banan == 0) 0 else if (banan == 1) 50 else if (banan == 2) 100 else "null"
      val two_two = if (person_risk == 0) 0 else if (person_risk == 1) 50 else if (person_risk == 2) 100 else "null"
      val three_three = if (enter_value > 40.0) 0 else if (enter_value <= 40.0 && enter_value > 20.0) 50 else if (enter_value <= 20.0) 100 else "null"
      val four_four = if (enter_risk == 1) 0 else if (enter_risk == 2) 50 else if (enter_risk == 3) 100 else "null"
      val end_end = (one_one.toString.toInt * 0.25) + (two_two.toString.toInt * 0.25) + (three_three.toString.toInt * 0.25) + (four_four.toString.toInt * 0.25)

      val end_value = if (end_end > 0.0 && end_end <= 25) "从宽审核" else if (end_end > 25 && end_end <= 50) "正常审核"
      else if (end_end > 50 && end_end <= 75) "审慎审核" else if (end_end > 75 && end_end <= 100) "建议拒绝"

      (ent_id+"", cert+"", end_value+"")
    }).toDF("ent_id","cert_no","model_conclusion").cache()

    //加载老版的数据，目的是与新版的进行匹配观察
    val url_one = "jdbc:mysql://172.16.11.105:3306/dwdb?user=root&password=bzn@cdh123!"
    val prop = new Properties
    val vc_verifyclaim = sQLContext.read.jdbc(url_one, "vc_verifyclaim", prop).select("insured_certno", "conclusion")
      .map(x => {
        val cert_no = x.getAs("insured_certno").toString
        val conclusion = x.getAs("conclusion").toString
        (cert_no, (cert_no, conclusion))
      }).filter(_._2._2 != null).reduceByKey((x1, x2) => {
      if (x1._1 == x2._1) x1 else x2
    }).map(x => (x._1+"", x._2._2+"")).toDF("cert_no", "conclusion")

    val end_ss = end_tep.join(vc_verifyclaim, "cert_no")
    end_ss.repartition(1).write.format("com.databricks.spark.csv")
      .option("header", "true") //在csv第一行有属性"true"，没有就是"false"
      .option("delimiter", ",") //默认以","分割
      .save("C:\\Users\\a2589\\Desktop\\end_model_predict")


  }
}
