package company.hbase_label.channel.channel_trait

import java.text.NumberFormat

import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by MK on 2018/11/5.
  */
trait cha_Insureinfo_until {


  //渠道人均保费:累计保费/累计投保人数



  //渠道首次投保至今月数
  def ent_fist_plc_month(before: RDD[(String, String)],
                       ods_ent_guzhu_salesman_channel: RDD[(String, String)],
                       sqlContext: HiveContext,
                       en_before: collection.Map[String, String],
                       str: String
                      )
  : RDD[(String, String, String)]
  = {
    import sqlContext.implicits._
    //标签
    val ent_id = before.map(x =>
      (x._1, x._2.split(";").filter(_.contains(str)).take(1).mkString(""))
    ).filter(_._2.length > 1)
      .map(end => {
        val before = JSON.parseObject(end._2)
        (end._1, before.getString("value"), before.getString("qual"))
      }).toDF("ent_name", "value", "qual")

    val end: RDD[(String, String, String)] = ods_ent_guzhu_salesman_channel.toDF("channel_name", "ent_name").join(ent_id, "ent_name")
      .map(x => (x.getAs[String]("channel_name"), x.getAs[String]("value").toInt))
      .reduceByKey((x1, x2) => if (x1 > x2) x1 else x2)
      .map(x => {
        (en_before.getOrElse(x._1, "null"), s"${x._2.toInt}", str)
      })
    end
  }


  //连续在保月份，都有哪个月
  def channel_add_month(before: RDD[(String, String)],
                        ods_ent_guzhu_salesman_channel: RDD[(String, String)],
                        sqlContext: HiveContext,
                        en_before: collection.Map[String, String],
                        str: String
                       )
  : RDD[(String, String, String)]
  = {
    import sqlContext.implicits._
    //标签
    val ent_id = before.map(x =>
      (x._1, x._2.split(";").filter(_.contains(str)).take(1).mkString(""))
    ).filter(_._2.length > 1)
      .map(end => {
        val before = JSON.parseObject(end._2)
        (end._1, before.getString("value"), before.getString("qual"))
      }).toDF("ent_name", "value", "qual")

    val end: RDD[(String, String, String)] = ods_ent_guzhu_salesman_channel.toDF("channel_name", "ent_name").join(ent_id, "ent_name")
      .map(x =>
        (x.getAs[String]("channel_name"), x.getAs[String]("value")))
      .reduceByKey((x1, x2) => x1 + "-" + x2)
      .map(x => {
        (en_before.getOrElse(x._1, "null"), x._2.split("-").distinct.mkString("-"), str)
      })
    end
  }

  //渠道月均保费
  def avg_month_premium(
                         ent_summary_month_1: DataFrame,
                         get_hbase_key_name: collection.Map[String, String],
                         sqlContext: HiveContext,
                         ods_ent_guzhu_salesman_channel: RDD[(String, String)],
                         en_before: collection.Map[String, String],
                         before: RDD[(String, String)],
                         str: String
                       )
  : RDD[(String, String, String)]
  = {
    val total_premium_data = before.map(x =>
      (x._1, x._2.split(";").filter(_.contains(str)).take(1).mkString(""))
    ).filter(_._2.length > 1)
      .map(end => {
        val before = JSON.parseObject(end._2)
        (before.getString("row"), before.getString("value"), before.getString("qual"))
      })

    import sqlContext.implicits._
    val tepOne = total_premium_data.map(x => (x._1, x._2.replaceAll(",", "").toDouble))
    val tepTwo = ent_summary_month_1.filter("data_type='totalInsuredPersons'").where("length(ent_id)>0").select("ent_id").map(x => (x.getString(0), 1)).reduceByKey(_ + _).map(x => (x._1, x._2))
    val end_result = tepOne.join(tepTwo).map(x => {
      val ent_id = x._1
      (s"${get_hbase_key_name.getOrElse(ent_id, "null")}", x._2._1.toString, x._2._2.toString)
    }).toDF("ent_name", "number_tb", "number_total")

    //根据企业名称进行join
    val end: RDD[(String, String, String)] = end_result.join(ods_ent_guzhu_salesman_channel.toDF("channel_name", "ent_name"), "ent_name").map(x => {
      val channel_name = x.getAs[String]("channel_name")
      val number_tb = x.getAs[String]("number_tb").toDouble
      val number_total = x.getAs[String]("number_total").toDouble
      (channel_name, (number_tb, number_total))
    })
      //    根据渠道名称进行分组
      .reduceByKey((x1, x2) => {
      val number_tb = x1._1 + x2._1
      val number_total = x1._2 + x2._2
      (number_tb, number_total)
    })
      .map(x => {
        val channel_name_id = en_before.getOrElse(x._1, "null")
        val number_tb = x._2._1
        val number_total = x._2._2
        val result = number_tb / number_total
        (channel_name_id, result + "", "avg_month_premium")
      })
    end

  }


  //渠道投保人员占总人数比
  def insured_rate(ods_policy_detail: DataFrame,
                   ods_policy_insured_detail: DataFrame,
                   ent_sum_level: DataFrame,
                   get_hbase_key_name: collection.Map[String, String],
                   sqlContext: HiveContext,
                   ods_ent_guzhu_salesman_channel: RDD[(String, String)],
                   en_before: collection.Map[String, String]

                  )
  : RDD[(String, String, String)]
  = {
    import sqlContext.implicits._
    val tepOne = ods_policy_detail.where("policy_status in('0','1', '7', '9', '10')").select("ent_id", "policy_id")
    val tepTwo = ods_policy_insured_detail.filter("insure_policy_status='1'").select("policy_id", "insured_cert_no")
    val tepThree = tepOne.join(tepTwo, "policy_id").filter("length(ent_id)>0")
    val tepFour = tepThree.map(x => {
      (x.getString(1), x.getString(2))
    }).reduceByKey((x1, x2) => {
      val res = x1 + "\t" + x2
      res
    }).map(x => {
      val number = x._2.split("\t").distinct.length
      (x._1, number)
    })
    val ent_sum_level_data = ent_sum_level.select("ent_id", "ent_scale").map(x => (x.getString(0), x.getString(1).toInt)).reduceByKey((x1, x2) => if (x1 >= x2) x1 else x2)

    val end_result = tepFour.join(ent_sum_level_data).map(x => {
      val number_tb = x._2._1
      val number_total = x._2._2
      (s"${get_hbase_key_name.getOrElse(x._1, "null")}", number_tb.toString, number_total.toString)
    }).toDF("ent_name", "number_tb", "number_total")


    //根据企业名称进行join
    val end: RDD[(String, String, String)] = end_result.join(ods_ent_guzhu_salesman_channel.toDF("channel_name", "ent_name"), "ent_name").map(x => {
      val channel_name = x.getAs[String]("channel_name")
      val number_tb = x.getAs[String]("number_tb").toDouble
      val number_total = x.getAs[String]("number_total").toDouble
      (channel_name, (number_tb, number_total))
    })
      //根据渠道名称进行分组
      .reduceByKey((x1, x2) => {
      val number_tb = x1._1 + x2._1
      val number_total = x1._2 + x2._2
      (number_tb, number_total)
    })
      .map(x => {
        val channel_name_id = en_before.getOrElse(x._1, "null")
        val number_tb = x._2._1
        val number_total = x._2._2
        val result = number_tb.toFloat / number_total.toFloat * 100
        (channel_name_id, result + "%", "insured_rate")
      })
    end

  }


  //渠道工种|赔付金额，累加比较
  def channel_add_type(before: RDD[(String, String)],
                       ods_ent_guzhu_salesman_channel: RDD[(String, String)],
                       sqlContext: HiveContext,
                       en_before: collection.Map[String, String],
                       str: String
                      )
  : RDD[(String, String, String)]
  = {
    import sqlContext.implicits._
    //标签
    val ent_id = before.map(x =>
      (x._1, x._2.split(";").filter(_.contains(str)).take(1).mkString(""))
    ).filter(_._2.length > 1)
      .map(end => {
        val before = JSON.parseObject(end._2)
        (end._1, before.getString("value").split("\\|")(0), before.getString("value").split("\\|")(1), before.getString("qual"))
      }).toDF("ent_name", "type", "numbers", "qual")

    val end: RDD[(String, String, String)] = ods_ent_guzhu_salesman_channel.toDF("channel_name", "ent_name").join(ent_id, "ent_name")
      .map(x => (x.getAs[String]("channel_name"), (x.getAs[String]("type"), x.getAs[String]("numbers").toDouble)))
      .reduceByKey((x1, x2) => if (x1._2 > x2._2) x1 else x2)
      .map(x => {
        (en_before.getOrElse(x._1, "null"), s"${x._2._1}|${x._2._2}", str)
      })
    end
  }


  //渠道月均在保人数
  def ent_month_plc_persons(ent_summary_month_1: DataFrame,
                            get_hbase_key_name: collection.Map[String, String],
                            sqlContext: HiveContext,
                            ods_ent_guzhu_salesman_channel: RDD[(String, String)],
                            en_before: collection.Map[String, String]
                           )
  : RDD[(String, String, String)]
  = {
    import sqlContext.implicits._
    val end_result = ent_summary_month_1.filter("data_type='totalInsuredPersons'").select("ent_id", "data_val", "month_id").map(x => {
      (x.getString(0), (x.getString(1).toDouble, x.getString(2)))
      //ent_id | 对data_val求和 | 求出month_id的次数，不去重
    }).reduceByKey((x1, x2) => {
      val sum = x1._1 + x2._1
      val count = x1._2 + "\t" + x2._2
      (sum, count)
    }).map(x => {
      val sizes = x._2._2.split("\t").length
      (s"${get_hbase_key_name.getOrElse(x._1, "null")}", x._2._1.toString, sizes.toString)
    }).toDF("ent_name", "nums", "size")

    //根据企业名称进行join
    val end = end_result.join(ods_ent_guzhu_salesman_channel.toDF("channel_name", "ent_name"), "ent_name").map(x => {
      val channel_name = x.getAs[String]("channel_name")
      val nums = x.getAs[String]("nums").toDouble
      val size = x.getAs[String]("size").toDouble
      (channel_name, (nums, size))
    })
      //根据渠道名称进行分组
      .reduceByKey((x1, x2) => {
      val nums = x1._1 + x2._1
      val size = x1._2 + x2._2
      (nums, size)
    })
      .map(x => {
        val channel_name_id = en_before.getOrElse(x._1, "null")
        val nums = x._2._1
        val size = x._2._2
        val res = nums / size
        (channel_name_id, res.toInt + "", "ent_month_plc_persons")
      })
    end

  }


  //月均增减员次数
  def ent_month_regulation_times(
                                  ent_add_regulation_times_data: RDD[(String, String, String)],
                                  ent_summary_month_1: DataFrame,
                                  get_hbase_key_name: collection.Map[String, String],
                                  sqlContext: HiveContext,
                                  ods_ent_guzhu_salesman_channel: RDD[(String, String)],
                                  en_before: collection.Map[String, String]
                                )
  : RDD[(String, String, String)]
  = {
    import sqlContext.implicits._

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
    val end_result = ent_add_regulation_times_data.map(x => {
      (x._1, x._2)
    }).join(month_number).map(x => {
      //企业ID，累计增减员次数，月份ID次数
      val ent_id = x._1
      val nums = x._2._1
      val months = x._2._2
      (s"${get_hbase_key_name.getOrElse(ent_id, "null")}", nums.toString, months.toString)

    }).toDF("ent_name", "nums", "months")

    //根据企业名称进行join
    val end = end_result.join(ods_ent_guzhu_salesman_channel.toDF("channel_name", "ent_name"), "ent_name").map(x => {
      val channel_name = x.getAs[String]("channel_name")
      val nums = x.getAs[String]("nums").toDouble
      val months = x.getAs[String]("months").toDouble
      (channel_name, (nums, months))
    })
      //根据渠道名称进行分组
      .reduceByKey((x1, x2) => {
      val nums = x1._1 + x2._1
      val months = x1._2 + x2._2
      (nums, months)
    })
      .map(x => {
        val channel_name_id = en_before.getOrElse(x._1, "null")
        val nums = x._2._1
        val months = x._2._2
        val res = nums / months
        (channel_name_id, Math.ceil(res).toInt + "", "ent_month_regulation_times")
      })
    end

  }

}
