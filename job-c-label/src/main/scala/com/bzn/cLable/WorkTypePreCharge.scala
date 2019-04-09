package com.bzn.cLable

import java.text.{NumberFormat, SimpleDateFormat}
import java.util.regex.Pattern
import java.util.{Calendar, Date}

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object WorkTypePreCharge {
  def main(args: Array[String]): Unit = {
    //得到标签数据
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf_spark = new SparkConf().setAppName(getClass.getName)
    conf_spark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf_spark.set("spark.sql.broadcastTimeout", "36000")
//      .setMaster("local[4]")

    val sc: SparkContext = new SparkContext(conf_spark)
    val sqlContext: HiveContext = new HiveContext(sc)

    import sqlContext.implicits._

    val yearData = getYear(sqlContext)
//    yearData.show(100)
    val monthData = getMonth(sqlContext)

    val res = yearData.unionAll(monthData).select("insured_work_type","count","charges")
      .map(x => {
        val insured_work_type = x.getAs[String]("insured_work_type")
        val count = x.getAs[String]("count").replace(",","").toDouble
        val charges = x.getAs[String]("charges").replace(",","").toDouble
        (insured_work_type,(count,charges))
      })
      .reduceByKey((x1,x2) => {
        val count = x1._1
        val charge = x1._2+x2._2
        (count,charge)
      })
        .map(x => {
          (x._1,x._2._1,x._2._2)
        })
      .toDF("insured_work_type","count","charges")
//    res.show(100)

    val outputTmpDir = "/share/work_type_pre_charge"
    val output = "odsdb_prd.work_type_pre_charge"

    res.rdd.map(x => x.mkString("\001")).repartition(1).saveAsTextFile(outputTmpDir)
    sqlContext.sql(s"""load data  inpath '$outputTmpDir' overwrite into table $output""")

  }

  /**
    * 得到月费
    * @param sqlContext
    */
  def getMonth(sqlContext:SQLContext) = {
    import sqlContext.implicits._
    // 创建一个数值格式化对象(对数字)
    var numberFormat = NumberFormat.getInstance
    // 设置精确到小数点后2位
    numberFormat.setMaximumFractionDigits(2)

    val ods_policy_detail = sqlContext.sql("select policy_id,policy_code,effect_date from odsdb_prd.ods_policy_detail")
      //      .filter("policy_status in ('0','1')")
      .filter("effect_date <= '2019-03-22 00:00:00'").cache()
    val ods_policy_insured_detail = sqlContext.sql("select policy_id,insured_work_type from odsdb_prd.ods_policy_insured_detail")

    val resOne = ods_policy_insured_detail.join(ods_policy_detail,ods_policy_insured_detail("policy_id") ===ods_policy_detail("policy_id"),"leftouter" )
      .select("policy_code","insured_work_type")
      .filter("length(policy_code) > 0")
      .filter("length(insured_work_type) > 0")

    val ods_policy_product_plan = sqlContext.sql("select policy_code,sku_charge_type,sku_price from odsdb_prd.ods_policy_product_plan")
      .filter("length(sku_price)>0 and sku_charge_type=1").cache()
      .select("policy_code","sku_charge_type","sku_price")
      .toDF("policy_code_a","sku_charge_type","sku_price")

//    val employer_liability_claims = sqlContext.sql("select policy_no,final_payment,pre_com from odsdb_prd.employer_liability_claims")
//      .map(x => x)
//      .filter(x => {
//        val str = x.getAs[String]("pre_com")
//        val p = Pattern.compile("[\u4e00-\u9fa5]")
//        val m = p.matcher(str)
//        if (!m.find) true else false
//      })
//      .map(x => {
//        val policy_no = x.getAs[String]("policy_no")
//        val final_payment = x.getAs[String]("final_payment")
//        val pre_com = x.getAs[String]("pre_com")
//        //如果最终赔付金额有值，就取最终金额，否则就取预估金额
//        var res = if (final_payment == "" || final_payment == null) pre_com else if (final_payment != "" || final_payment != null) final_payment else ""
//        (policy_no, res)
//      })
//      .filter(_._2 != ".")
//      .filter(_._2 != "#N/A")
//      .toDF("policy_no_b","preCom")
//      .cache

    val resTwo = resOne.join(ods_policy_product_plan,resOne("policy_code") ===ods_policy_product_plan("policy_code_a"))
      .select("policy_code","insured_work_type","sku_price")
      .map(x=> {
        val policy_code = x.getAs[String]("policy_code")
        val insured_work_type = x.getAs[String]("insured_work_type")
        val sku_price = x.getAs[String]("sku_price")
        (policy_code,insured_work_type,sku_price)
      })
      .toDF("policy_code","insured_work_type","charges")
      .map(x => {
        val insured_work_type = x.getAs[String]("insured_work_type")
        var charges = x.getAs[String]("charges")
        if(charges == null ||  charges == ""){
          charges = "0"
        }
        val chargesres = charges.replace(",","").toDouble
        (insured_work_type,(chargesres,1))
      })
      .reduceByKey((x1,x2) => {
        val charge = x1._1+x2._1
        val count = x1._2+x2._2
        (charge,count)
      })
      .map(x => {
        val count = numberFormat.format(x._2._2)
        val charges = numberFormat.format(x._2._1)
        (x._1,count,charges)
      })
      .toDF("insured_work_type","count","charges")

    resTwo
  }


  /**
    * 得到年费
    * @param sqlContext
    * @return
    */
  def getYear(sqlContext:SQLContext)={

    import sqlContext.implicits._
    // 创建一个数值格式化对象(对数字)
    var numberFormat = NumberFormat.getInstance
    // 设置精确到小数点后2位
    numberFormat.setMaximumFractionDigits(2)

    val ods_policy_detail = sqlContext.sql("select policy_id,policy_code,effect_date from odsdb_prd.ods_policy_detail")
      //      .filter("policy_status in ('0','1')")
      .filter("effect_date <= '2019-03-22 00:00:00'").cache()
    val ods_policy_insured_detail = sqlContext.sql("select policy_id,insured_work_type,insured_start_date,insured_end_date from odsdb_prd.ods_policy_insured_detail")
      .filter("length(insured_start_date) >0 and length(insured_end_date)>0")

    val resOne = ods_policy_insured_detail.join(ods_policy_detail,ods_policy_insured_detail("policy_id") ===ods_policy_detail("policy_id"),"leftouter")
      .select("policy_code","insured_work_type","insured_start_date","insured_end_date")
      .filter("length(policy_code) > 0")
      .filter("length(insured_work_type) > 0")

    val ods_policy_product_plan = sqlContext.sql("select policy_code,sku_charge_type,sku_price from odsdb_prd.ods_policy_product_plan")
      .filter("length(sku_price)>0 and sku_charge_type=2").cache()
      .select("policy_code","sku_charge_type","sku_price")
      .toDF("policy_code_a","sku_charge_type","sku_price")

//    val employer_liability_claims = sqlContext.sql("select policy_no,final_payment,pre_com from odsdb_prd.employer_liability_claims")
//      .map(x => x)
//      .filter(x => {
//        val str = x.getAs[String]("pre_com")
//        val p = Pattern.compile("[\u4e00-\u9fa5]")
//        val m = p.matcher(str)
//        if (!m.find) true else false
//      })
//      .map(x => {
//        val policy_no = x.getAs[String]("policy_no")
//        val final_payment = x.getAs[String]("final_payment")
//        val pre_com = x.getAs[String]("pre_com")
//        //如果最终赔付金额有值，就取最终金额，否则就取预估金额
//        var res = if (final_payment == "" || final_payment == null) pre_com else if (final_payment != "" || final_payment != null) final_payment else ""
//        (policy_no, res)
//      })
//      .filter(_._2 != ".")
//      .filter(_._2 != "#N/A")
//      .toDF("policy_no_b","preCom")
//      .cache
    val resTwo = resOne.join(ods_policy_product_plan,resOne("policy_code") ===ods_policy_product_plan("policy_code_a"))
      .select("policy_code","insured_work_type","insured_start_date","insured_end_date","sku_price")
      .map(x=> {
        val policy_code = x.getAs[String]("policy_code")
        val insured_work_type = x.getAs[String]("insured_work_type")
        val insured_start_date = x.getAs("insured_start_date").toString.split(" ")(0).replaceAll("-", "").replaceAll("/", "")
        val insured_end_date = x.getAs("insured_end_date").toString.split(" ")(0).replaceAll("-", "").replaceAll("/", "")
        var sku_price = x.getAs[String]("sku_price")
        if(sku_price == null || sku_price == ""){
          sku_price ="0"
        }
        val res_sku_price = sku_price.toDouble
        val days = getBeg_End_one_two(insured_start_date,insured_end_date).size.toDouble
        val charges = numberFormat.format(((days/365)*res_sku_price).toDouble).replace(",","")
        (policy_code,insured_work_type,charges)
      })

      .toDF("policy_code","insured_work_type","charges")
      .filter("charges <> '∞'" )
      .map(x => {
        val insured_work_type = x.getAs[String]("insured_work_type")
        val charges = x.getAs[String]("charges").toDouble
        (insured_work_type,(charges,1))
      })
      .reduceByKey((x1,x2) => {
        val charge = x1._1+x2._1
        val count = x1._2+x2._2
        (charge,count)
      })
      .map(x => {
        val count = numberFormat.format(x._2._2)
        val charges = numberFormat.format(x._2._1)
        (x._1,count,charges)
      })
      .toDF("insured_work_type","count","charges")

      resTwo
  }

  //得到2个日期之间的所有天数
  def getBeg_End_one_two(mon3: String, day_time: String): ArrayBuffer[String]
  = {
    val sdf = new SimpleDateFormat("yyyyMMdd")

    //得到过去第三个月的日期
    val c = Calendar.getInstance
    c.setTime(new Date)
    c.add(Calendar.MONTH, -3)
    val m3 = c.getTime

    //得到今天的日期
    val cc = Calendar.getInstance
    cc.setTime(new Date)
    val day = cc.getTime

    //得到他们相间的所有日期
    val arr: ArrayBuffer[String] = ArrayBuffer[String]()
    val date_start = sdf.parse(mon3)
    //    val date_start = sdf.parse("20161007")
    val date_end = sdf.parse(day_time)
    //    val date_end = sdf.parse("20161008")
    var date = date_start
    val cd = Calendar.getInstance //用Calendar 进行日期比较判断

    while (date.getTime <= date_end.getTime) {
      arr += sdf.format(date)
      cd.setTime(date)
      cd.add(Calendar.DATE, 1); //增加一天 放入集合
      date = cd.getTime
    }
    arr

  }
}
