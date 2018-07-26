package company.sparkMLlib.another

import java.io.StringReader

import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MK on 2018/5/28.
  */
object sum_avg {

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
    val banan_path = "C:\\Users\\a2589\\Desktop\\需求one\\统计\\报案.csv"
    val person_path = "C:\\Users\\a2589\\Desktop\\需求one\\统计\\个人.csv"
    val enter_path = "C:\\Users\\a2589\\Desktop\\需求one\\统计\\企业.csv"


    import sQLContext.implicits._
    val baoan = table(sc, sQLContext, banan_path)
    val baoan_end = baoan.map(x => {
      (x(0).toString, x(1).toString, x(2).toString, x(3).toString)
    }).toDF("身份证", "报案时效特征", "报案时效模型预测值", "报案时效老版值").cache

    val person = table(sc, sQLContext, person_path)
    val person_end = person.map(x => {
      (x(0).toString, x(1).toString, x(2).toString, x(3).toString)
    }).toDF("身份证", "个人风险等级", "个人风险特征值", "企业ID").cache

    val enter = table(sc, sQLContext, enter_path)
    val enter_end = enter.map(x => {
      (x(0).toString, x(1).toString, x(2).toString, x(3).toString, x(4).toString, x(5).toString, x(6).toString)
    }).toDF("企业ID", "企业价值模型预测值", "老版企业价值", "企业风险模型预测值", "老版企业风险", "企业价值特征", "企业风险特征")


    val tep_one: DataFrame = baoan_end.join(person_end, "身份证").cache
    val tep_two_end = tep_one.join(enter_end, "企业ID")

    tep_two_end.map(x => {
      val ent_id = x.getAs[String]("企业ID")
      val banan = x.getAs[String]("报案时效模型预测值").toInt
      val person_risk = x.getAs[String]("个人风险等级")
      val enter_value = x.getAs[String]("企业价值模型预测值").toDouble
      val enter_risk = x.getAs[String]("企业风险模型预测值").toInt
      //
      val one_one = if (banan == 0) 0 else if (banan == 1) 50 else if (banan == 2) 100 else "null"
      val two_two = if (person_risk == "低") 0 else if (person_risk == "中") 50 else if (person_risk == "高") 100 else "null"
      val three_three = if (enter_value > 40.0) 0 else if (enter_value <= 40.0 && enter_value > 20.0) 50 else if (enter_value <= 20.0) 100 else "null"
      val four_four = if (enter_risk == 1) 0 else if (enter_risk == 2) 50 else if (enter_risk == 3) 100 else "null"

      (one_one, two_two, three_three, four_four, ent_id)
    }).filter(x => {
      if (x._1.toString != "null" && x._2.toString != "null" && x._3.toString != "null" && x._4 != "null") true else false
    }).map(x => {
      val one_one = x._1
      val two_two = x._2
      val three_three = x._3
      val four_four = x._4
      val ent_id = x._5

      val end_end = (one_one.toString.toInt * 0.25) + (two_two.toString.toInt * 0.25) + (three_three.toString.toInt * 0.25) + (four_four.toString.toInt * 0.25)
      val end_value = if (end_end > 0.0 && end_end <= 25) "从宽审核" else if (end_end > 25 && end_end <= 50) "正常审核"
      else if (end_end > 50 && end_end <= 75) "审慎审核" else if (end_end > 75 && end_end <= 100) "建议拒绝"

      val ss = end_value.toString
      (ss, ent_id)
    }).toDF("最终值", "企业ID").show

    //    val res = tep_two_end.join(hehe, "企业ID")
    //    res.repartition(1).write.format("com.databricks.spark.csv")
    //      .option("header", "true") //在csv第一行有属性"true"，没有就是"false"
    //      .option("delimiter", ",") //默认以","分割
    //      .save("C:\\Users\\a2589\\Desktop\\汇总")
  }
}
