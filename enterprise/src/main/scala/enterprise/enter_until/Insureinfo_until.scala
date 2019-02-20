package enterprise.enter_until

import java.text.{NumberFormat, SimpleDateFormat}
import java.util.{Date, Properties}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable

/**
  * Created by a2589 on 2018/4/3.
  */
trait Insureinfo_until {
  //累计增减员次数
  def ent_add_regulation_times(ods_policy_detail: DataFrame, ods_policy_preserve_detail: DataFrame): RDD[(String, String, String)] = {
    //preserve_status：4：已处理，5：已生效
    val tepOne = ods_policy_detail.select("ent_id", "policy_id")
    val tepTwo = ods_policy_preserve_detail.where("preserve_status in ('4','5')").select("policy_id", "preserve_id")
    val tepThree = tepOne.join(tepTwo, "policy_id").filter("LENGTH(ent_id)>0")
    //      .show()
    //    |           policy_id|              ent_id|  preserve_id|
    //    |0a40acb3d5f0450c9...|c956cae36ecd4f0d9...|BQ17030210037|
    val end: RDD[(String, String, String)] = tepThree.map(x => {
      (x.getString(1), x.getString(2))
    }).reduceByKey((x1, x2) => {
      val sum = x1 + "\t" + x2
      sum
    }).map(x => {
      val count = x._2.split("\t").distinct.length
      (x._1, count + "", "ent_add_regulation_times")
    })
    end
  }

  //月均增减员次数
  def ent_month_regulation_times(ent_add_regulation_times_data: RDD[(String, String, String)], ent_summary_month_1: DataFrame): RDD[(String, String, String)] = {
    //totalInsuredPersons：当月在保人数
    val total = ent_summary_month_1.filter("data_type='totalInsuredPersons'").select("ent_id", "month_id")
    //统计月份ID的次数
    val month_number = total.map(x => {
      (x.getString(0), x.getString(1))
    }).reduceByKey((x1, x2) => {
      val sum = x1 + "\t" + x2
      sum
    }).map(x => {
      //ent_id | month_id出现的次数
      (x._1, x._2.split("\t").length)
    }).filter(_._2 > 0)

    val end: RDD[(String, String, String)] = ent_add_regulation_times_data.map(x => {
      (x._1, x._2)
    }).join(month_number).map(x => {
      //企业ID，累计增减员次数，月份ID次数
      val ent_id = x._1
      val nums = x._2._1
      val months = x._2._2
      val res = nums.toDouble / months.toDouble
      (ent_id, Math.ceil(res) + "", "ent_month_regulation_times")
    })
    end
    //      .take(10).foreach(println(_))

  }

  //累计增员人数
  def ent_add_sum_persons(ent_summary_month: DataFrame): RDD[(String, String, String)] = {
    val end: RDD[(String, String, String)] = ent_summary_month.where("data_type in('monAddPersons','monRenewalPersons')").select("ent_id", "data_val")
      .map(x => {
        (x.getString(0), x.getString(1).toInt)
      }).reduceByKey(_ + _).map(x => {
      (x._1, x._2 + "", "ent_add_sum_persons")
    })
    end
  }

  //累计减员人数
  def ent_del_sum_persons(ent_summary_month: DataFrame): RDD[(String, String, String)] = {
    val end: RDD[(String, String, String)] = ent_summary_month.where("data_type='monSubPersons'").select("ent_id", "data_val")
      .map(x => {
        (x.getString(0), x.getString(1).toInt)
      }).reduceByKey(_ + _).map(x => {
      (x._1, x._2 + "", "ent_del_sum_persons")
    })
    end

  }

  //月均在保人数
  def ent_month_plc_persons(ent_summary_month_1: DataFrame): RDD[(String, String, String)] = {
    val end: RDD[(String, String, String)] = ent_summary_month_1.filter("data_type='totalInsuredPersons'").select("ent_id", "data_val", "month_id").map(x => {
      (x.getString(0), (x.getString(1).toDouble, x.getString(2)))
      //ent_id | 对data_val求和 | 求出month_id的次数，不去重
    }).reduceByKey((x1, x2) => {
      val sum = x1._1 + x2._1
      val count = x1._2 + "\t" + x2._2
      (sum, count)
    }).map(x => {
      val sizes = x._2._2.split("\t").length
      val res = x._2._1 / sizes
      (x._1, res.toInt + "", "ent_month_plc_persons")
    })
    end
  }

  //续投人数
  def ent_continuous_plc_persons(ent_summary_month: DataFrame): RDD[(String, String, String)] = {
    val end: RDD[(String, String, String)] = ent_summary_month.where("data_type='monRenewalPersons'").select("ent_id", "data_val")
      .map(x => {
        (x.getString(0), x.getString(1).toInt)
      }).reduceByKey(_ + _).map(x => {
      (x._1, x._2 + "", "ent_continuous_plc_persons")
    })
    end
    //      .take(10).foreach(println(_))

  }

  //投保工种数
  def ent_insure_craft(ods_policy_detail: DataFrame, ods_policy_insured_detail: DataFrame): RDD[(String, String, String)] = {
    val tepOne = ods_policy_detail.where("policy_status in ('0','1','7','9','10')").select("ent_id", "policy_id")
    val tepTwo = ods_policy_insured_detail.filter("length(insured_work_type)>0").select("policy_id", "insured_work_type")

    val tepThree = tepOne.join(tepTwo, "policy_id").filter("LENGTH(ent_id)>0")
    //      .show()
    //    |           policy_id|              ent_id|insured_work_type|
    //    |ac45c5f06fd646f49...|37b640c3661d4cef8...|         板材加工/操作工|
    //end_id | 工种去重后出现的次数
    val end: RDD[(String, String, String)] = tepThree.map(x => {
      (x.getString(1), x.getString(2))
    })
      .reduceByKey((x1, x2) => {
        val sum = x1 + "\t" + x2
        sum
      }).map(x => {
      val s = x._2.split("\t").distinct.length
      (x._1, s + "", "ent_insure_craft")
    })
    end
  }

  //求出该企业中第一工种出现的类型哪个最多
  def ent_first_craft(ods_policy_insured_detail: DataFrame, ods_policy_detail: DataFrame, d_work_level: DataFrame): RDD[(String, String, String)] = {
    val tepOne = ods_policy_insured_detail.select("policy_id", "insured_cert_no", "insured_work_type", "insure_policy_status")
    val tepTwo = ods_policy_detail.select("policy_id", "ent_id")
    val tepThree = d_work_level.select("work_type", "ai_level")
    val ss = tepOne.join(tepTwo, "policy_id")
    //      .show()
    //    |         policy_id|   insured_cert_no|insured_work_type|insure_policy_status|              ent_id|
    //    |122008268007673856|44132219951005661X|              服务员|                   1|10e11fd0b1a7488db...|
    val result = ss.join(tepThree, ss("insured_work_type") === tepThree("work_type")).where("insure_policy_status='1' and ai_level is not null")
      .filter("LENGTH(ent_id)>0")
    //      .show()
    //    |         policy_id|   insured_cert_no|insured_work_type|insure_policy_status|              ent_id|work_type|ai_level|
    //    |122008268007673856|44132219951005661X|              服务员|                   1|10e11fd0b1a7488db...|      服务员|       1|

    //求出在该企业第一工种出现最多的次数
    val end = result.map(x => {
      ((x.getString(4), x.getString(2)), 1)
    }).reduceByKey(_ + _).map(x => {
      (x._1._1, (x._1._2, x._2))
    }).groupByKey().flatMap(x => {
      val ent_first_craft_opt = x._2.toArray.sortBy(_._2)(Ordering[Int].reverse).lift(0)
      // 返回工种列表
      List((x._1, ent_first_craft_opt))
    }).map(x => {
      if (x._2.isDefined) {
        (x._1, s"${x._2.get._1}|${x._2.get._2}", "ent_first_craft")
      } else {
        ("", "", "")
      }
    }).filter(_._1 != "")

    end
  }

  //求出该企业中第二工种出现的类型哪个最多
  def ent_second_craft(ods_policy_insured_detail: DataFrame, ods_policy_detail: DataFrame, d_work_level: DataFrame): RDD[(String, String, String)] = {
    val tepOne = ods_policy_insured_detail.select("policy_id", "insured_cert_no", "insured_work_type", "insure_policy_status")
    val tepTwo = ods_policy_detail.select("policy_id", "ent_id")
    val tepThree = d_work_level.select("work_type", "ai_level")
    val ss = tepOne.join(tepTwo, "policy_id")
    //      .show()
    //    |         policy_id|   insured_cert_no|insured_work_type|insure_policy_status|              ent_id|
    //    |122008268007673856|44132219951005661X|              服务员|                   1|10e11fd0b1a7488db...|
    val result = ss.join(tepThree, ss("insured_work_type") === tepThree("work_type")).where("insure_policy_status='1' and ai_level is not null")
      .filter("LENGTH(ent_id)>0")
    //      .show()
    //    |         policy_id|   insured_cert_no|insured_work_type|insure_policy_status|              ent_id|work_type|ai_level|
    //    |122008268007673856|44132219951005661X|              服务员|                   1|10e11fd0b1a7488db...|      服务员|       1|

    //求出在该企业第二工种出现最多的次数

    val end = result.map(x => {
      ((x.getString(4), x.getString(2)), 1)
    }).reduceByKey(_ + _).map(x => {
      (x._1._1, (x._1._2, x._2))
    }).groupByKey().flatMap(x => {
      val ent_second_craft_opt = x._2.toArray.sortBy(_._2)(Ordering[Int].reverse).lift(1)
      // 返回工种列表
      List((x._1, ent_second_craft_opt))
    }).map(x => {
      if (x._2.isDefined) {
        (x._1, s"${x._2.get._1}|${x._2.get._2}", "ent_second_craft")
      } else {
        ("", "", "")
      }
    }).filter(_._1 != "")

    end
  }

  //求出该企业中第三工种出现的类型哪个最多
  def ent_third_craft(ods_policy_insured_detail: DataFrame, ods_policy_detail: DataFrame, d_work_level: DataFrame): RDD[(String, String, String)] = {
    val tepOne = ods_policy_insured_detail.select("policy_id", "insured_cert_no", "insured_work_type", "insure_policy_status")
    val tepTwo = ods_policy_detail.select("policy_id", "ent_id")
    val tepThree = d_work_level.select("work_type", "ai_level")
    val ss = tepOne.join(tepTwo, "policy_id")
    //      .show()
    //    |         policy_id|   insured_cert_no|insured_work_type|insure_policy_status|              ent_id|
    //    |122008268007673856|44132219951005661X|              服务员|                   1|10e11fd0b1a7488db...|
    val result = ss.join(tepThree, ss("insured_work_type") === tepThree("work_type")).where("insure_policy_status='1' and ai_level is not null")
      .filter("LENGTH(ent_id)>0")
    //      .show()
    //    |         policy_id|   insured_cert_no|insured_work_type|insure_policy_status|              ent_id|work_type|ai_level|
    //    |122008268007673856|44132219951005661X|              服务员|                   1|10e11fd0b1a7488db...|      服务员|       1|

    //求出在该企业第三工种出现最多的次数
    val end = result.map(x => {
      ((x.getString(4), x.getString(2)), 1)
    }).reduceByKey(_ + _).map(x => {
      (x._1._1, (x._1._2, x._2))
    }).groupByKey().flatMap(x => {
      val ent_third_craft_opt = x._2.toArray.sortBy(_._2)(Ordering[Int].reverse).lift(2)
      // 返回工种列表
      List((x._1, ent_third_craft_opt))
    }).map(x => {
      if (x._2.isDefined) {
        (x._1, s"${x._2.get._1}|${x._2.get._2}", "ent_third_craft")
      } else {
        ("", "", "")
      }
    }).filter(_._1 != "")

    end
  }

  //该企业中哪个工种类型的赔额额度最高
  def ent_most_money_craft(ods_policy_insured_detail: DataFrame, ods_policy_detail: DataFrame, d_work_level: DataFrame, employer_liability_claims: DataFrame): RDD[(String, String, String)] = {
    val tepOne = ods_policy_insured_detail.select("policy_id", "insured_cert_no", "insured_work_type", "insure_policy_status")
    val tepTwo = ods_policy_detail.select("policy_id", "ent_id", "policy_code")
    val tepThree = d_work_level.select("work_type", "ai_level")
    val tepFour = employer_liability_claims.select("policy_no", "final_payment").filter("LENGTH(final_payment)>0")
    val ss = tepOne.join(tepTwo, "policy_id")
    val result = ss.join(tepThree, ss("insured_work_type") === tepThree("work_type")).where("insure_policy_status='1' and ai_level is not null")
      .filter("LENGTH(ent_id)>0")
    //      .show()
    //    |           policy_id|   insured_cert_no|insured_work_type|insure_policy_status|              ent_id|         policy_code|work_type|ai_level|
    //    |  155459451304939520|370281199110107321|             出库检验|                   1|ddc05b821562470fa...|  HL1100000098000823|     出库检验|       3|
    val tepFive = result.join(tepFour, result("policy_code") === tepFour("policy_no"))
    //      .show()
    //    |         policy_id|   insured_cert_no|insured_work_type|insure_policy_status|              ent_id|       policy_code|work_type|ai_level|         policy_no|final_payment|
    //    |155108461842141184|429005198910293127|           一般制造工人|                   1|50e586760e8548009...|HL1100000201001374|   一般制造工人|       4|HL1100000201001374|             |
    //根据企业ID进行分组，并找出对应集合中的赔偿额度最高的工种
    val end: RDD[(String, String, String)] = tepFive.select("ent_id", "work_type", "final_payment").map(x => {
      (x.getString(0), (x.getString(1), x.get(2).toString.toDouble))
    }).reduceByKey((x1, x2) => {
      val result = if (x1._2 > x2._2) x1 else x2
      result
    }).map(x => (x._1, s"${x._2._1}|${x._2._2}", "ent_most_money_craft"))
    end
  }

  //该企业中哪个工种类型出险最多
  def ent_most_count_craft(ods_policy_insured_detail: DataFrame, ods_policy_detail: DataFrame, employer_liability_claims: DataFrame): RDD[(String, String, String)] = {
    val tepOne = ods_policy_insured_detail.select("policy_id", "insured_cert_no", "insured_work_type", "insure_policy_status")
    val tepTwo = ods_policy_detail.select("policy_id", "ent_id", "policy_code")
    val tepFour = employer_liability_claims.select("policy_no")
    //该企业中有多少人交保
    val ss = tepOne.join(tepTwo, "policy_id")
    val result = ss.where("insure_policy_status='1'").filter("LENGTH(ent_id)>0")
    //      .show()
    //    |           policy_id|   insured_cert_no|insured_work_type|insure_policy_status|              ent_id|         policy_code|work_type|ai_level|
    //    |  155459451304939520|370281199110107321|             出库检验|                   1|ddc05b821562470fa...|  HL1100000098000823|     出库检验|       3|
    //该企业中都有那些人出险了
    val tepFive = result.join(tepFour, result("policy_code") === tepFour("policy_no"))
    //      .show()
    //    |           policy_id|   insured_cert_no|insured_work_type|insure_policy_status|              ent_id|       policy_code|         policy_no|
    //    |8bb8f0332a114ee3a...|411323198901180550|              操作工|                   1|facc09ada8cb4a518...|900000046544362325|900000046544362325|
    //    ent_most_count_craft

    //找出哪种类型的工种出险最多
    val end: RDD[(String, String, String)] = tepFive.select("ent_id", "insured_work_type").map(x => {
      ((x.getString(0), x.getString(1)), 1)
    }).reduceByKey(_ + _).map(x => {
      //ent_id | 工种类型|根据企业ID和工种类型分组后出现的次数
      (x._1._1, (x._1._2, x._2))
    }).reduceByKey((x1, x2) => {
      //根据次数进行比较，找到最多的那个工种
      val res = if (x1._2 > x2._2) {
        x1
      } else {
        x2
      }
      res
    }).map(x => {
      (x._1, s"${x._2._1}|${x._2._2}", "ent_most_count_craft")
    })
    end
  }

  //投保人员占总人数比
  def insured_rate(ods_policy_detail: DataFrame, ods_policy_insured_detail: DataFrame, ent_sum_level: DataFrame): RDD[(String, String, String)] = {
    val tepOne = ods_policy_detail.where("policy_status in('0','1', '7', '9', '10')").select("ent_id", "policy_id")
    val tepTwo = ods_policy_insured_detail.filter("insure_policy_status='1'").select("policy_id", "insured_cert_no")
    val tepThree = tepOne.join(tepTwo, "policy_id").filter("length(ent_id)>0")
    //      .show()
    //    |           policy_id|              ent_id|   insured_cert_no|
    //    |f777bc98ed1644c7a...|bfdfb02642d043328...|433125200109143147|
    //该企业有多少人投保了
    // 创建一个数值格式化对象(对数字)
    val numberFormat = NumberFormat.getInstance
    val tepFour = tepThree.map(x => {
      (x.getString(1), x.getString(2))
    }).reduceByKey((x1, x2) => {
      val res = x1 + "\t" + x2
      res
    }).map(x => {
      //身份证出现的次数
      val number = x._2.split("\t").distinct.length
      //ent_id
      (x._1, number)
    })
    //该企业一共多少人
    val ent_sum_level_data = ent_sum_level.select("ent_id", "ent_scale").map(x => (x.getString(0), x.getString(1).toInt)).reduceByKey((x1, x2) => if (x1 >= x2) x1 else x2)

    val end: RDD[(String, String, String)] = tepFour.join(ent_sum_level_data).map(x => {
      val number_tb = x._2._1
      val number_total = x._2._2
      //      val result = numberFormat.format(number_tb.toFloat / number_total.toFloat * 100)
      val result = number_tb.toFloat / number_total.toFloat * 100
      (x._1, result + "%", "insured_rate")
    })
    end
    //      .take(10).foreach(println(_))

  }

  //有效保单数
  def effective_policy(ods_policy_detail: DataFrame): RDD[(String, String, String)] = {
    val tepOne = ods_policy_detail.where("policy_status in('0','1','7','9','10') and ent_id!=''").select("ent_id")
    val end: RDD[(String, String, String)] = tepOne.map(x => {
      (x.getString(0), 1)
    }).reduceByKey(_ + _).map(x => {
      (x._1, x._2 + "", "effective_policy")
    })
    end
  }

  //累计投保人次
  def total_insured_count(ods_policy_detail: DataFrame, ods_policy_insured_detail: DataFrame): RDD[(String, String, String)] = {
    val tepOne = ods_policy_detail.where("policy_status in('0','1', '7', '9', '10')").select("ent_id", "policy_id")
    val tepTwo = ods_policy_insured_detail.select("policy_id")
    val tepThree = tepOne.join(tepTwo, "policy_id").filter("length(ent_id)>0")
    //      .show()
    //    |           policy_id|              ent_id|
    //    |cc1025c8708746dbb...|365104828040476c9...|

    val end: RDD[(String, String, String)] = tepThree.map(x => {
      (x.getString(1), 1)
    }).reduceByKey(_ + _).map(x => {
      (x._1, x._2 + "", "total_insured_count")
    })
    end
  }

  //累计投保人数 totalInsuredPersons（去重）对身份证号去重
  def total_insured_persons(ods_policy_detail: DataFrame, ods_policy_insured_detail: DataFrame): RDD[(String, String, String)] = {
    val tepOne = ods_policy_detail.where("policy_status in('0','1', '7', '9', '10')").select("ent_id", "policy_id")
    val tepTwo = ods_policy_insured_detail.select("policy_id", "insure_policy_status", "insured_cert_no")
    val tepThree = tepOne.join(tepTwo, "policy_id")
    //      .show()
    //    |           policy_id|              ent_id|insure_policy_status|   insured_cert_no|
    //    |f5f06d9bd3544efdb...|e760c5bca4034dca8...|                   1|412829198006134810|

    //对企业ID进行分组，同时对身份证进行去重求个数,就是当前再保人数
    val end: RDD[(String, String, String)] = tepThree.map(x => {
      (x.getString(1), x.getString(3))
    }).reduceByKey((x1, x2) => {
      val res = x1 + "\t" + x2
      res
    }).map(x => {
      (x._1, x._2.split("\t").distinct.length + "", "total_insured_persons")
    })
    end
  }

  //旧当前在保人数
  def cur_insured_persons(ods_policy_detail: DataFrame, ods_policy_insured_detail: DataFrame): RDD[(String, String, String)] = {
    //        val tepOne = ods_policy_detail.where("policy_status in('0','1', '7', '9', '10')").select("ent_id", "policy_id")
    val tepOne = ods_policy_detail.where("policy_status = '1'").select("ent_id", "policy_id")
    val tepTwo = ods_policy_insured_detail.select("policy_id", "insure_policy_status", "insured_cert_no")
    val tepThree = tepOne.join(tepTwo, "policy_id").filter("length(ent_id)>0 and insure_policy_status='1'")
    //      .show()
    //    |           policy_id|              ent_id|insure_policy_status|   insured_cert_no|
    //    |f5f06d9bd3544efdb...|e760c5bca4034dca8...|                   1|412829198006134810|

    //对企业ID进行分组，同时对身份证进行去重求个数,就是当前再保人数
    val end: RDD[(String, String, String)] = tepThree.map(x => {
      (x.getString(1), x.getString(3))
    }).reduceByKey((x1, x2) => {
      val res = x1 + "\t" + x2
      res
    }).map(x => {
      (x._1, x._2.split("\t").distinct.length + "", "cur_insured_persons")
    })
    end
  }

  //新的当前在保人数
  def read_people_product(sqlContext: HiveContext, location_mysql_url: String,
                          prop: Properties,
                          location_mysql_url_dwdb: String)
  : RDD[(String, (Int, Int))] = {

    val now: Date = new Date
    val dateFormatOne: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val now_Date = dateFormatOne.format(now)


    import sqlContext.implicits._
    val dim_1 = sqlContext.read.jdbc(location_mysql_url_dwdb, "dim_product", prop).select("product_code", "dim_1").where("dim_1 in ('外包雇主','骑士保','大货车')").map(x => x.getAs[String]("product_code")).collect
    //    val dim_1 = sqlContext.sql("select * from dim_product").select("product_code", "dim_1").where("dim_1 in ('外包雇主','骑士保','大货车')").map(x => x.getAs[String]("product_code")).collect

    val ods_policy_detail: DataFrame = sqlContext.read.jdbc(location_mysql_url, "ods_policy_detail", prop).where("policy_status in ('1','0')").select("ent_id", "policy_id", "insure_code")
    //    val ods_policy_detail: DataFrame = sqlContext.sql("select * from ods_policy_detail").where("policy_status in ('1','0')").select("ent_id", "policy_id", "insure_code")

    val tep_ods_one = ods_policy_detail.map(x => (x.getAs[String]("insure_code"), x)).filter(x => if (dim_1.contains(x._1)) true else false)
      .map(x => {
        (x._2.getAs[String]("ent_id"), x._2.getAs[String]("policy_id"), x._2.getAs[String]("insure_code"))
      }).toDF("ent_id", "policy_id", "insure_code").cache

    val end = sqlContext.read.jdbc(location_mysql_url, "ods_policy_curr_insured", prop).join(tep_ods_one, "policy_id").map(x => {
      ((x.getAs[String]("ent_id"), x.getAs[String]("day_id")), x.getAs[Long]("curr_insured").toInt)
    }).filter(_._1._2.toDouble <= now_Date.toDouble)

    val end_all = end.reduceByKey(_ + _).map(x => ((x._1._1, x._1._2.substring(0, 6)), x._2)).reduceByKey((x1, x2) => if (x1 >= x2) x1 else x2)
      .map(x => (x._1._1, (x._1._2.toInt, x._2)))
      .reduceByKey((x1, x2) => if (x1._1 >= x2._1) x1 else x2)
    end_all
  }

  //累计保费
  def total_premium(ods_policy_detail: DataFrame): RDD[(String, String, String)] = {
    val numberFormat = NumberFormat.getInstance
    numberFormat.setMaximumFractionDigits(2)

    val tepOne = ods_policy_detail.filter("policy_status in('0','1', '7', '9', '10') and ent_id!=''").select("ent_id", "premium")
    //      .show()
    //    |              ent_id|    premium|
    //    |0a789d56b7444d519...| 62905.1200|
    val end: RDD[(String, String, String)] = tepOne.map(x => {
      (x.getString(0), x.get(1).toString.toDouble)
    }).reduceByKey(_ + _).map(x => {
      //      (x._1, numberFormat.format(x._2), "total_premium")
      (x._1, x._2.toString, "total_premium")
    })
    end
  }

  //月均保费
  def avg_month_premium(total_premium_data: RDD[(String, String, String)], ent_summary_month_1: DataFrame): RDD[(String, String, String)] = {
    val numberFormat = NumberFormat.getInstance
    numberFormat.setMaximumFractionDigits(2)

    //得到企业交的总保费：企业ID| 总保费
    val tepOne = total_premium_data.map(x => {
      (x._1, x._2.replaceAll(",", "").toDouble)
    })

    //求出我表中有哪些企业在保，同时求出我A企业投保的次数 ?
    //data_type: ->当月在保人数（totalInsuredPersons）| ->当前累计投保保费（totalPremium）
    //ent_id | 出现的次数
    val tepTwo = ent_summary_month_1.filter("data_type='totalInsuredPersons'").where("length(ent_id)>0").select("ent_id").map(x => {
      (x.getString(0), 1)
    }).reduceByKey(_ + _).map(x => {
      (x._1, x._2)
    })
    val end: RDD[(String, String, String)] = tepOne.join(tepTwo).map(x => {
      val ent_id = x._1
      //使用总保费/次数 (就是每月的保费)
      val avg_month_premium = x._2._1 / x._2._2
      //      (ent_id, numberFormat.format(avg_month_premium), "avg_month_premium")
      (ent_id, avg_month_premium.toString, "avg_month_premium")
    })
    end
  }

  //连续在保月数
  def ent_continuous_plc_month(ent_summary_month_1: DataFrame): RDD[(String, String, String)] = {

    //求出我表中有哪些企业在保，然后将人数大于0的过滤出来，同时对企业进行分组，将month_id集合以|进行隔离
    //data_type: ->当月在保人数（totalInsuredPersons）| ->当前累计投保保费（totalPremium）
    val end: RDD[(String, String, String)] = ent_summary_month_1.filter("data_type='totalInsuredPersons'").select("month_id", "data_val", "ent_id").map(x => {
      (x.getString(0), x.getString(1), x.getString(2))
    }).filter(_._2.toInt > 0).map(x => {
      //ent_id | month_id
      (x._3, x._1)
    }).reduceByKey((x1, x2) => {
      val res = x1 + "-" + x2
      res
    }).map(x => {
      (x._1, x._2, "ent_continuous_plc_month")
    })

    end
  }

  //连续在保月数,同上但因为都有用到会有问题，因此区分开
  def ent_continuous_plc_month_number(ent_summary_month_1: DataFrame): RDD[(String, String, String)] = {

    //求出我表中有哪些企业在保，然后将人数大于0的过滤出来，同时对企业进行分组，将month_id集合以|进行隔离
    //data_type: ->当月在保人数（totalInsuredPersons）| ->当前累计投保保费（totalPremium）
    val end: RDD[(String, String, String)] = ent_summary_month_1.filter("data_type='totalInsuredPersons'").select("month_id", "data_val", "ent_id").map(x => {
      (x.getString(0), x.getString(1), x.getString(2))
    }).filter(_._2.toInt > 0).map(x => {
      //ent_id | month_id
      (x._3, x._1)
    }).reduceByKey((x1, x2) => {
      val res = x1 + "-" + x2
      res
    }).map(x => {
      (x._1, x._2, "month_number")
    })

    end
  }


  //最多的次数
  def maxNumber(res: List[String]): String = {
    val list = res

    var map = new mutable.HashMap[String, Int]()
    for (x <- list) {
      if (map.get(x).isEmpty) {
        map.put(x, 1)
      }
      else {
        val t = map.get(x)
        map.put(x, t.get + 1)
      }
    }
    var max = -1
    var key = ""
    for (x <- map) {
      if (x._2 > max) {
        key = x._1
        max = x._2
      }
    }
    key

  }


  //  //得到当前的时间
  //  def getNowTime(): String = {
  //    //得到当前的日期
  //    val now: Date = new Date()
  //    val dateFormatOne: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd 00:00:00")
  //    val now_Date: String = dateFormatOne.format(now)
  //    now_Date
  //  }

  //  //将时间转换为时间戳
  //  def currentTimeL(str: String): Long = {
  //    val format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
  //
  //    val insured_create_time_curr_one: DateTime = DateTime.parse(str, format)
  //    val insured_create_time_curr_Long: Long = insured_create_time_curr_one.getMillis
  //
  //    insured_create_time_curr_Long
  //  }

}
