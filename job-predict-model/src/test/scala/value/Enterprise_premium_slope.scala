package value

import java.text.{NumberFormat, SimpleDateFormat}
import java.util.Date

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MK on 2018/5/4.
  * 企业保费，平均增长率，已在oozie上每天更新表
  */
object Enterprise_premium_slope {

  //  读取ods_policy_insured_charged_vt，对(企业，月份)分组，找到企业每月的保费存入表中
  def ent_permium(sqlContext: HiveContext): Unit
  = {
    //得到当前**年**月,并在下面进行过滤
    val day = new Date()
    val df = new SimpleDateFormat("yyyyMM")
    val year_month = df.format(day).toInt
    val numberFormat = NumberFormat.getInstance
    // 设置精确到小数点后2位
    numberFormat.setMaximumFractionDigits(2)

    import sqlContext.implicits._
    sqlContext.sql("select * from odsdb_prd.ods_policy_insured_charged_vt").filter("length(ent_id)>0")
      .map(x => {
        val ent_id = x.getAs("ent_id").toString
        val day_id = x.getAs("day_id").toString
        val new_day_id = day_id.substring(0, day_id.length - 2)
        val sku_day_price = x.getAs("sku_day_price").toString.toDouble
        ((ent_id, new_day_id.toInt), sku_day_price)
      }).reduceByKey(_ + _).sortBy(_._1._2).map(x => {
      //ent_id,该企业每月的保费,月份
      (x._1._1, x._2.toString, x._1._2.toString)
    }).map(x => {
      /**
        * 对企业进行分组，找到我该企业每月的所对应的保费，并根据最小二乘求得斜率
        **/
      val ent_id = x._1
      val premium = x._2
      val month = x._3
      //该企业每月和每月对应的保费
      (ent_id, (month, premium, s"$month|$premium"))
    })
      .filter(_._2._1.toInt < year_month)
      .reduceByKey((x1, x2) => {
        val month = x1._1 + "\t" + x2._1
        val premium = x1._2 + "\t" + x2._2
        val month_premium = x1._3 + "\t" + x2._3
        (month, premium, month_premium)
      }).filter(_._2._1.split("\t") != 1)
      .map(f = x => {
        val tepOne = x._2._3.split("\t")
        val x_Coordinate_axis = x._2._1.split("\t")
        //求得总保费
        val y_Coordinate_axis = x._2._2.split("\t").map(_.toDouble).sum.formatted("%.2f")

        //找到最早一次的日期
        val before_date = tepOne(0).split("\\|")(0)
        //找到最近的一次日期
        val after_date = tepOne(tepOne.length - 1).split("\\|")(0)


        //找到最早一次的日期所对应的保费
        val before = tepOne(0).split("\\|")(1).toDouble
        //找到最近的一次日期多对应的保费
        val after = tepOne(tepOne.length - 1).split("\\|")(1).toDouble
        //开放根数
        val open_Root = x_Coordinate_axis.length - 1.toDouble
        //求得平均增长率
        val three = if (after == before) 0.00 else Math.pow(after / before, 1 / open_Root) - 1


        val three_end = if (tepOne.length >= 3) {
          //保费
          val end = tepOne(tepOne.length - 1).split("\\|")(1).toDouble
          val end_two = tepOne(tepOne.length - 3).split("\\|")(1).toDouble

          val tt = Math.pow(end / end_two, 1 / 2.0) - 1
          val res = numberFormat.format(tt * 100) + "%"
          res
        } else "0%"

        //企业的ID，每个月的保费(这里取得的是首次和最后，这里的最后要小于当月)，同上只不过算的是月份，平均增长率,最近3个月的平均增长率，企业的累计保费
        (x._1, s"${before.formatted("%.2f")}|${after.formatted("%.2f")}", s"$before_date|$after_date", numberFormat.format(three * 100) + "%", three_end, y_Coordinate_axis)
      })
      .toDF("ent_id", "premium", "month", "slope", "three_slope", "premium_all")
      .insertInto("odsdb_prd.enterprise_premium_slope", overwrite = true)
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")

    val conf_s = new SparkConf().setAppName("wuYu").setMaster("local[2]")
    val sc = new SparkContext(conf_s)
    val sqlContext: HiveContext = new HiveContext(sc)

    /**
      * 读取ods_policy_insured_charged_vt，对(企业，月份)分组，找到企业每月的保费存入表中
      * 同时对enterprise_premium_slope这张表计算企业的平均增长率
      **/
    ent_permium(sqlContext: HiveContext)
  }
}
