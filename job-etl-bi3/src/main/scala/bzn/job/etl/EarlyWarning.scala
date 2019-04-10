package bzn.job.etl

import java.io.File
import java.sql.DriverManager
import java.util.Properties

import com.alibaba.fastjson.JSONObject
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by MK on 2018/7/16.
  * 雇主预警
  */
object EarlyWarning extends early_until {
  //遍历某目录下所有的文件和子文件
  def subDir(dir: File): Iterator[File]
  = {
    val dirs = dir.listFiles().filter(_.isDirectory())
    val files = dir.listFiles().filter(_.isFile())
    files.toIterator ++ dirs.toIterator.flatMap(subDir)
  }

  def getC3p0DateSource(path: String, table_name: String, url: String): Boolean
  = {
    Class.forName("com.mysql.jdbc.Driver")
    //获取连接//http://baidu.com
    val connection = DriverManager.getConnection(url)
    //通过连接创建statement
    var statement = connection.createStatement()
    val sql1 = s"DELETE FROM odsdb.$table_name WHERE day_id='${getDay.split(" ")(0).replace("-", "")}'"

    val sql2 = s"load data infile '$path'  into table odsdb.$table_name fields terminated by ';'"
    statement = connection.createStatement()
    //先删除数据，在导入数据
    statement.execute(sql1)
    statement.execute(sql2)
  }

  //toMysql
  def toMsql(bzn_year: RDD[String], path_hdfs: String, path: String, table_name: String, url: String): Unit
  = {
    bzn_year.repartition(1).saveAsTextFile(path_hdfs)

    //得到我目录中的该文件
    val res_file = for (d <- subDir(new File(path))) yield {
      if (d.getName.contains("-") && !d.getName.contains(".")) d.getName else "null"
    }
    //得到part-0000
    val end = res_file.filter(_ != "null").mkString("")
    //通过load,将数据加载到MySQL中 : /share/ods_policy_insured_charged_vt/part-0000
    val tep_end = path + "/" + end
    getC3p0DateSource(tep_end, table_name, url)
  }

  //多事件合并
  def merge(sqlContext :SQLContext,ods_ent_guzhu_salesman_channel:DataFrame,mid_guzhu_ent_label: DataFrame, ods_policy_detail: DataFrame, employer_liability_claims: DataFrame, ods_policy_premium_detail: DataFrame)
  = {
    import sqlContext.implicits._
    val all = mid_guzhu_ent_label.map(x => {
      //企业名称
      val ent_name: String = x.getAs[String]("ent_name")
      //前日
      val yesterday_insured_persons: Int = x.getAs[Int]("yesterday_insured_persons")
      //当日
      val cur_insured_persons: Int = x.getAs[Int]("cur_insured_persons")
      //上月在保峰值
      val lastmonth_insured_persons_max: Int = x.getAs[Int]("lastmonth_insured_persons_max")


      val addPersons: Double = cur_insured_persons.toDouble - yesterday_insured_persons.toDouble
//      val ringRatio: Double = if (yesterday_insured_persons == 0) 0.00 else addPersons / yesterday_insured_persons
      (
        ent_name,
        yesterday_insured_persons,
        cur_insured_persons,
        addPersons,
//        ringRatio,
        lastmonth_insured_persons_max
      )
    })

    val allTemp = all.toDF("ent_name","yesterday_insured_persons","cur_insured_persons","addPersons","lastmonth_insured_persons_max")

    val guzhuRes: RDD[(String, Int, Int, Double, Double, Int)] = ods_ent_guzhu_salesman_channel.join(allTemp,ods_ent_guzhu_salesman_channel("ent_name") === allTemp("ent_name"))
      .map(x => {
        val new_channel_name = x.getAs[String]("new_channel_name")
        val yesterday_insured_persons = x.getAs[Int]("yesterday_insured_persons")
        val cur_insured_persons = x.getAs[Int]("cur_insured_persons")
        val addPersons = x.getAs[Double]("addPersons")
        val lastmonth_insured_persons_max = x.getAs[Int]("lastmonth_insured_persons_max")
        (new_channel_name,(yesterday_insured_persons,cur_insured_persons,addPersons,lastmonth_insured_persons_max))
      })
      .reduceByKey((x1,x2) => {
      val yesterday_insured_persons: Int = x1._1+x2._1
      val cur_insured_persons: Int = x1._2+x2._2
      val addPersons: Double = x1._3+x2._3
      val lastmonth_insured_persons_max: Int = x1._4+x2._4
      (yesterday_insured_persons,cur_insured_persons,addPersons,lastmonth_insured_persons_max)
    })
      .map(x=>{
        val ringRatio: Double = if (x._2._1 == 0) 0.00 else x._2._3 / x._2._1
        (x._1,x._2._1.toInt,x._2._2.toInt,x._2._3.toDouble,ringRatio,x._2._4.toInt)
      })
//    guzhuRes.foreach(println)
    //增员
    val add_person = guzhuRes.filter(x =>
      //人数差值||人数差值/上日人数
      if (x._4 >= 200 && x._5 >= 0.1) true else false).map(x => {
      val json_data = new JSONObject()
      val end = if (x._5 >= 0.5) {

        json_data.put("entName", x._1)
        json_data.put("yesterdayInsuredPersons", x._2)
        json_data.put("curInsuredPersons", x._3)
        json_data.put("addPersons", x._4)
        json_data.put("ringRatio", x._5.formatted("%.4f"))
        json_data.put("level", "二级")
        json_data.put("name", "大幅增员预警")
        json_data
      } else {
        json_data.put("entName", x._1)
        json_data.put("yesterdayInsuredPersons", x._2)
        json_data.put("curInsuredPersons", x._3)
        json_data.put("addPersons", x._4)
        json_data.put("ringRatio", x._5.formatted("%.4f"))
        json_data.put("level", "一级")
        json_data.put("name", "大幅增员预警")
        json_data
      }

      s"$getUUID;ADD;${x._1};${getDay.split(" ")(0).replace("-", "")};;$end;$getDay;${x._4}"

    })
    //减员
    val subtraction_person = guzhuRes.filter(x => if (x._4 <= -200 && x._5 <= -0.1) true else false).map(x => {
      val json_data = new JSONObject()
      val end = if (x._3 <= 10) {
        json_data.put("entName", x._1)
        json_data.put("yesterdayInsuredPersons", x._2)
        json_data.put("curInsuredPersons", x._3)
        json_data.put("addPersons", x._4)
        json_data.put("ringRatio", x._5.formatted("%.4f"))
        json_data.put("level", "三级")
        json_data.put("name", "大幅减员预警")
        json_data
      } else if (x._5 <= -0.5) {
        json_data.put("entName", x._1)
        json_data.put("yesterdayInsuredPersons", x._2)
        json_data.put("curInsuredPersons", x._3)
        json_data.put("addPersons", x._4)
        json_data.put("ringRatio", x._5.formatted("%.4f"))
        json_data.put("level", "二级")
        json_data.put("name", "大幅减员预警")
        json_data
      } else {
        json_data.put("entName", x._1)
        json_data.put("yesterdayInsuredPersons", x._2)
        json_data.put("curInsuredPersons", x._3)
        json_data.put("addPersons", x._4)
        json_data.put("ringRatio", x._5.formatted("%.4f"))
        json_data.put("level", "一级")
        json_data.put("name", "大幅减员预警")
        json_data
      }
      s"$getUUID;DEL;${x._1};${getDay.split(" ")(0).replace("-", "")};;$end;$getDay;${x._4}"

    })

    //在保人数预警
    val guarantee_person = guzhuRes.filter(_._6 != 0).filter(x => {
      //当前在保 / 上月峰值
      val end = x._3.toDouble / x._6.toDouble
      if (end <= 0.7) true else false
    }).map(x => {
      val json_data = new JSONObject()
      //当前在保 / 上月峰值
      val end = x._3.toDouble / x._6.toDouble
      val result = if (end >= 0 && end <= 0.2) {
        json_data.put("entName", x._1)
        json_data.put("yesterdayInsuredPersons", x._2)
        json_data.put("curInsuredPersons", x._3)
        json_data.put("lmhInsuredPersonsMax", x._6)
        json_data.put("ringRatio", end.formatted("%.4f"))
        json_data.put("level", "三级")
        json_data.put("name", "在保人数预警")
        json_data
      } else if (end > 0.2 && end <= 0.5) {
        json_data.put("entName", x._1)
        json_data.put("yesterdayInsuredPersons", x._2)
        json_data.put("curInsuredPersons", x._3)
        json_data.put("lmhInsuredPersonsMax", x._6)
        json_data.put("ringRatio", end.formatted("%.4f"))
        json_data.put("level", "二级")
        json_data.put("name", "在保人数预警")
        json_data

      } else {
        json_data.put("entName", x._1)
        json_data.put("yesterdayInsuredPersons", x._2)
        json_data.put("curInsuredPersons", x._3)
        json_data.put("lmhInsuredPersonsMax", x._6)
        json_data.put("ringRatio", end.formatted("%.4f"))
        json_data.put("level", "一级")
        json_data.put("name", "在保人数预警")
        json_data
      }
      s"$getUUID;CUR_INSURED;${x._1};${getDay.split(" ")(0).replace("-", "")};;$result;$getDay;${x._3 - x._6}"
    })

    //通过企业名称关联得到保单号
    val tep_one = mid_guzhu_ent_label.join(ods_policy_detail, "ent_name").cache

    val customer_loss_tep_one = tep_one.join(ods_policy_premium_detail, "policy_code").map(x => {
      val ent_name = x.getAs[String]("ent_name")
      val add_premium = is_noy_null(x.getAs[java.math.BigDecimal]("add_premium"))
      val add_persons = is_noy_null(x.getAs[java.math.BigDecimal]("add_persons"))
      val del_premium = is_noy_null(x.getAs[java.math.BigDecimal]("del_premium"))
      val day_id = x.getAs[String]("day_id")
      (ent_name, (add_premium - del_premium, add_persons, day_id))
    }).reduceByKey((x1, x2) => {
      val date = x1._3 + ";" + x2._3
      //总保费
      val sum_premium = x1._1 + x2._1
      //累计人数
      val sum_person = x1._2 + x2._2
      (sum_premium, sum_person, date)
    })
      .map(x => {
        (x._1,x._2._1,x._2._2,x._2._3)
      })
      .toDF("ent_name","sum_premium","sum_person","date")
    val customerLossTepOneRes = ods_ent_guzhu_salesman_channel.join(customer_loss_tep_one,ods_ent_guzhu_salesman_channel("ent_name") === customer_loss_tep_one("ent_name"))
      .map(x => {
        val new_channel_name = x.getAs[String]("new_channel_name")
        val sum_premium = x.getAs[Double]("sum_premium")
        val sum_person = x.getAs[Double]("sum_person")
        val date = x.getAs[String]("date")
        (new_channel_name,(sum_premium,sum_person,date))
      })
      .reduceByKey((x1,x2) => {
        val sum_premium =x1._1 +x2._1
        val sum_person =x1._2 +x2._2
        val date =x1._3 + ";"+x2._3
        (sum_premium,sum_person,date)
      })
      .map(x=> {
        (x._1,(x._2._1.toDouble,x._2._2,x._2._3))
      })
      .map(x => {
      //企业名称，总保费，总人数，最早日期，最近日期
      val first_date = x._2._3.split(";").map(_.toInt).min //最早日期
      val last_date = x._2._3.split(";").map(_.toInt).max //最近日期

      (x._1, (x._2._1.formatted("%.2f").toDouble, x._2._2.toInt, if (first_date == null || first_date == 0 || first_date == " ") "" else first_date.toString, if (last_date == null || last_date == 0 || last_date == " ") "" else last_date.toString))
    })
    //客户流失预警
  val customer_loss = guzhuRes.filter(_._3 == 0).map(x => {
    val json_data = new JSONObject()
    json_data.put("entName", x._1)
    json_data.put("yesterdayInsuredPersons", x._2)
    json_data.put("curInsuredPersons", x._3)
    json_data.put("lmhInsuredPersonsMax", x._5)
    json_data.put("name", "客户流失预警")
    (x._1, json_data)
  }).join(customerLossTepOneRes).map(x => {
    x._2._1.put("firstInsureTime", x._2._2._3)
    x._2._1.put("lastOperTime", x._2._2._4)
    x._2._1.put("totalPremium", x._2._2._1)
    x._2._1.put("totalPersons", x._2._2._2)
    s"$getUUID;LOSS;${x._1};${getDay.split(" ")(0).replace("-", "")};;${x._2._1};$getDay;0"

  })

    //预警出最近7天死亡的案件保单
    val end_death = tep_one.join(employer_liability_claims, "policy_code").map(x => {
      //出险，找最近7天的
      val risk_date = x.getAs[String]("risk_date")
      (risk_date, x)
    }).filter(x => getBeg_End.contains(x._1)) //过滤出出险日期是最近7天的数据
      .map(x => {

      val json_data = new JSONObject()
      //企业名称
      val ent_name: String = x._2.getAs[String]("ent_name")
      //当日
      val cur_insured_persons: Int = x._2.getAs[Int]("cur_insured_persons")
      //已赚保费
      val chargedPremium = x._2.getAs[java.math.BigDecimal]("charged_premium")
      //赔付金额
      val payment = x._2.getAs[java.math.BigDecimal]("payment")
      //赔付率
      val payrate = x._2.getAs[java.math.BigDecimal]("payrate")
      //保单号
      val policyCode = x._2.getAs[String]("policy_code")
      //出险日期
      val risk_date = x._2.getAs[String]("risk_date")
      //保额
      val sku_coverage = x._2.getAs[String]("sku_coverage")

//      json_data.put("entName", ent_name)
//      json_data.put("curInsuredPersons", cur_insured_persons)
//      json_data.put("chargedPremium", chargedPremium)
//      json_data.put("payment", payment)
//      json_data.put("payrate", payrate)
//      json_data.put("policyCode", policyCode)
//      json_data.put("riskDate", risk_date)
//      json_data.put("baoe", sku_coverage)
//      json_data.put("name", "死亡案件预警")

      (ent_name,cur_insured_persons,chargedPremium,payment,payrate,policyCode,risk_date,sku_coverage)
    })
      .toDF("ent_name_two","cur_insured_persons","chargedPremium","payment","payrate","policyCode","risk_date","sku_coverage")

    val res = ods_ent_guzhu_salesman_channel.join(end_death,ods_ent_guzhu_salesman_channel("ent_name") ===end_death("ent_name_two"))
      .select("new_channel_name","cur_insured_persons","chargedPremium","payment","payrate","policyCode","risk_date","sku_coverage")

    val resTemp = res.map(x => {
      val json_data = new JSONObject()
      //企业名称
      val ent_name: String = x.getAs[String]("new_channel_name")
      //当日
      val cur_insured_persons: Int = x.getAs[Int]("cur_insured_persons")
      //已赚保费
      val chargedPremium = x.getAs[java.math.BigDecimal]("chargedPremium")
      //赔付金额
      val payment = x.getAs[java.math.BigDecimal]("payment")
      (ent_name,(cur_insured_persons,chargedPremium,payment,1))
    })
      .reduceByKey((x1,x2) => {
        val cur_insured_persons = x1._1+x2._1
        val chargedPremium = x1._2.add(x2._2)
        val payment = x1._3.add(x2._3)
        val countDeath = x1._4+x2._4
        (cur_insured_persons,chargedPremium,payment,countDeath)
      }).map(x => {
      val json_data = new JSONObject()
      //企业名称
      val ent_name: String = x._1
      //当日
      val cur_insured_persons: Int = x._2._1
      //已赚保费
      val chargedPremium = x._2._2.toString.toDouble
      //赔付金额
      val payment = x._2._3.toString.toDouble

      //赔付率
      var payrate = 0.0
      if(payment ==  0.0 ){
        payrate = 0.0
      }else  if(chargedPremium  == 0.0 )
        payrate = -1
      else{
        payrate = payment/chargedPremium
      }
      //死亡案件数
      val countDeath = x._2._4

      json_data.put("entName", ent_name)
      json_data.put("curInsuredPersons", cur_insured_persons)
      json_data.put("chargedPremium", chargedPremium)
      json_data.put("payment", payment)
      json_data.put("payrate", payrate)
      json_data.put("countDeath", countDeath)
      json_data.put("name", "死亡案件预警")
      s"$getUUID;DEATH;$ent_name;${getDay.split(" ")(0).replace("-", "")};;$json_data;$getDay;$countDeath"
    })

    val end_result: RDD[String] = add_person.union(subtraction_person).union(guarantee_person).union(customer_loss).union(resTemp)
    end_result

  }

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf_spark: SparkConf = new SparkConf().setAppName("piwik").set("spark.sql.broadcastTimeout", "36000")
//          .setMaster("local[4]")

    val sc = new SparkContext(conf_spark)
    val sqlContext: HiveContext = new HiveContext(sc)

    val lines: Iterator[String] = Source.fromURL(getClass.getResource("/config-scala.properties")).getLines
    val url: String = lines.filter(_.contains("location_mysql_url")).map(_.split("==")(1)).mkString("")
    val prop: Properties = new Properties
    import sqlContext.implicits._
    val mid_guzhu_ent_label: DataFrame = sqlContext.read.jdbc(url, "mid_guzhu_ent_label", prop)
//        .where("ent_name = '北京德众汇才企业管理有限公司'")
      .cache

    val ods_ent_guzhu_salesman_channel = sqlContext.read.jdbc(url, "ods_ent_guzhu_salesman", prop).map(x => {
      val ent_name = x.getAs[String]("ent_name").trim
      val channel_name = x.getAs[String]("channel_name").trim
      val new_channel_name = if (channel_name == "直客") ent_name else channel_name
      (new_channel_name, ent_name)
    })
      .toDF("new_channel_name","ent_name")


    val ods_policy_detail: DataFrame = sqlContext.read.jdbc(url, "ods_policy_detail", prop).selectExpr("policy_code", "holder_company as ent_name", "sku_coverage")
    val employer_liability_claims: DataFrame = sqlContext.read.jdbc(url, "employer_liability_claims", prop).selectExpr("policy_no as policy_code", "case_type", "risk_date").filter("case_type ='死亡'").cache
    val ods_policy_premium_detail: DataFrame = sqlContext.read.jdbc(url, "ods_policy_premium_detail", prop)

    val end = merge(sqlContext,ods_ent_guzhu_salesman_channel:DataFrame,mid_guzhu_ent_label, ods_policy_detail, employer_liability_claims, ods_policy_premium_detail)

//    end.foreach(println)
//    存入哪张表中
    val table_name = "mid_channel_warning_detail"

    /*
     * 存入mysql
     **/
    val tep_end = end
    //得到时间戳
    val timeMillions = System.currentTimeMillis
    //HDFS需要传的路径
    val path_hdfs = s"file:///share/${table_name}_$timeMillions"
    //本地需要传的路径
    val path = s"/share/${table_name}_$timeMillions"
    //每天新创建一个目录，将数据写入到新目录中
    toMsql(tep_end, path_hdfs, path, table_name, url)

  }
}
