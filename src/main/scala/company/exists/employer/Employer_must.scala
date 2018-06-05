package company.exists.employer

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MK on 2018/5/11.
  */
object Employer_must extends employ_until {

  def main(args: Array[String]): Unit = {
    val conf_s = new SparkConf().setAppName("wuYu").setMaster("local[2]")
    val sc = new SparkContext(conf_s)
    val sQLContext: SQLContext = new SQLContext(sc)

    /**
      * 1、非雇主通过保单号和批单号进行匹配
      **/
    //大表
    val path_big = "C:\\Users\\a2589\\Desktop\\需求one\\台账\\业务台账.csv"
    val tep_Twos: DataFrame = big_table(sc, sQLContext, path_big)
    //这一步主要时为了，显示的时候让其显示
    val tep_Two = tep_Twos.withColumn("原保单号", tep_Twos("保单号")).withColumn("原批单号", tep_Twos("批单号")).cache
    //小表
    val path = "C:\\Users\\a2589\\Desktop\\需求one\\是否是雇主\\非雇主.csv"
    val tep_ones = little_table(sc, sQLContext, path)
    //      .select("保单号", "批单号", "保费", "结算单金额")
    //这一步主要时为了，显示的时候让其显示
    val tep_one = tep_ones.withColumn("原保单号", tep_ones("保单号")).withColumn("原批单号", tep_ones("批单号")).cache

    val es = not_employer(tep_Two: DataFrame, tep_one: DataFrame, sQLContext: SQLContext)


    /**
      * 2、雇主通过保单和批单进行匹配
      **/
    //大表
    val path_big_employer: String = "C:\\Users\\a2589\\Desktop\\需求one\\蓝领外包\\渠道部蓝领外包业务台账.csv"
    val tep_Two_employers: DataFrame = big_table(sc, sQLContext, path_big_employer)
    //这一步主要时为了，显示的时候让其显示
    val tep_Two_employer = tep_Two_employers.withColumn("原保单号", tep_Two_employers("保单号")).withColumn("原批单号", tep_Two_employers("批单号")).cache


    //小表
    val path_employer = "C:\\Users\\a2589\\Desktop\\需求one\\是否是雇主\\雇主.csv"
    val tep_one_employers = little_table(sc, sQLContext, path_employer)
    //      .select("保单号", "批单号", "保费", "结算单金额")
    //这一步主要时为了，显示的时候让其显示
    val tep_one_employer = tep_one_employers.withColumn("原保单号", tep_one_employers("保单号")).withColumn("原批单号", tep_one_employers("批单号")).cache

    val all: DataFrame = tep_Two_employer.join(tep_one_employer, Seq("保单号", "批单号"))
    val end = employer_bd_pd(tep_Two_employer: DataFrame, tep_one_employer: DataFrame, all: DataFrame, sQLContext: SQLContext)

    /**
      * 3、非雇主找到我a表存在b表不存在的数据
      **/
    val one = not_gu_little(tep_one: DataFrame, tep_Two: DataFrame, sQLContext: SQLContext)
    /**
      * 4、雇主找到我a表存在b表不存在的数据
      **/
    val two = gu_little(tep_one_employer: DataFrame, tep_Two_employer: DataFrame, sQLContext: SQLContext)

    //非雇主通过(保单号和批单号)进行匹配 ,雇主通过(保单号和批单号进行匹配)后保存 
    not_employer_save(es, end, one, two)

  }
}
