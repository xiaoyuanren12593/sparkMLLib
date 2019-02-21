package bzn.label.channel_claiminfo

import java.text.NumberFormat
import java.util.regex.Pattern

import bzn.common.Until
import com.alibaba.fastjson.JSON
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by MK on 2018/11/1.
  */
trait ChaClaiminfoUntil extends Until {
  /**
    * 渠道风险等级
    **/
  def ent_work_risk_r_To_Hbase(conf: Configuration, conf_fs: Configuration,
                               tableName: String,
                               columnFamily1: String,
                               channel_before: RDD[(String, String)],
                               before: RDD[(String, String)],
                               sqlContext: HiveContext,
                               ods_ent_guzhu_salesman_channel: RDD[(String, String)],
                               en_before: collection.Map[String, String]
                              ): Unit
  = {

    import sqlContext.implicits._

    def while_value(work_type: String): RDD[(String, String, String)] = {

      //标签
      val ent_id = before.map(x =>
        (x._1, x._2.split(";").filter(_.contains(work_type)).take(1).mkString(""))
      ).filter(_._2.length > 1)
        .map(end => {
          val before = JSON.parseObject(end._2)
          //企业，和企业对应的数据
          (end._1, before.getString("value"), before.getString("qual"))
        }).toDF("ent_name", "value", "qual")

      val end: RDD[(String, String, String)] = ods_ent_guzhu_salesman_channel.toDF("channel_name", "ent_name").join(ent_id, "ent_name")
        .map(x => (x.getAs[String]("channel_name"), x.getAs[String]("value").toInt))
        .reduceByKey((x1, x2) => x1 + x2)
        .map(x => (en_before.getOrElse(x._1, "null"), x._2.toString, work_type))
      end
    }

    val zero_f = while_value("worktype_0_count").filter(_._1.length > 5)
    val one_f = while_value("worktype_1_count").filter(_._1.length > 5)
    val two_f = while_value("worktype_2_count").filter(_._1.length > 5)
    val three_f = while_value("worktype_3_count").filter(_._1.length > 5)
    val four_f = while_value("worktype_4_count").filter(_._1.length > 5)
    val five_f = while_value("worktype_5_count").filter(_._1.length > 5)
    val six_f = while_value("worktype_6_count").filter(_._1.length > 5)
    val seven_f = while_value("worktype_7_count").filter(_._1.length > 5)

    toHbase(zero_f, columnFamily1, "ent_work_risk_zero", conf_fs, tableName, conf)
    toHbase(one_f, columnFamily1, "ent_work_risk_one", conf_fs, tableName, conf)
    toHbase(two_f, columnFamily1, "ent_work_risk_two", conf_fs, tableName, conf)
    toHbase(three_f, columnFamily1, "ent_work_risk_three", conf_fs, tableName, conf)
    toHbase(four_f, columnFamily1, "ent_work_risk_four", conf_fs, tableName, conf)
    toHbase(five_f, columnFamily1, "ent_work_risk_five", conf_fs, tableName, conf)
    toHbase(six_f, columnFamily1, "ent_work_risk_six", conf_fs, tableName, conf)
    toHbase(seven_f, columnFamily1, "ent_work_risk_seven", conf_fs, tableName, conf)

  }


  /**
    * 计算2个日期相隔多好天
    *
    * @param date3 前一个日期
    * @param date4 后一个日期
    * @return 相隔天数
    **/
  def xg(date3: String, date4: String): Int
  = {
    import java.text.SimpleDateFormat
    val format = new SimpleDateFormat("yyyy/MM/dd")
    val date1 = format.parse(date3)
    val date2 = format.parse(date4)

    val a = ((date1.getTime - date2.getTime) / (1000 * 3600 * 24)).asInstanceOf[Int]
    a.abs
  }


  /**
    * 渠道月均出现概率,每百人月均出险概率（逻辑改为:  每百人出险人数=总出险概率*100）
    *
    * @param employer_liability_claims_r    出险表
    * @param ods_policy_detail_r            企业表
    * @param ods_policy_insured_detail_r    保全表
    * @param get_hbase_key_name             得到标签的key和企业明
    * @param sqlContext                     sqlContext
    * @param ods_ent_guzhu_salesman_channel 渠道表
    * @param en_before                      渠道名渠道ID
    * @return key value ent_monthly_risk
    **/
  def ent_monthly_risk(employer_liability_claims_r: DataFrame,
                       ods_policy_detail_r: DataFrame,
                       ods_policy_insured_detail_r: DataFrame,
                       get_hbase_key_name: collection.Map[String, String],
                       sqlContext: HiveContext,
                       ods_ent_guzhu_salesman_channel: RDD[(String, String)],
                       en_before: collection.Map[String, String]
                      )
  : RDD[(String, String, String)]
  = {

    import sqlContext.implicits._
    //求出对应企业中有多少人出险了（怎么求出？我要计算出该出险保单所在的是哪个企业，使用的是join）
    val employer_liability_claims = employer_liability_claims_r.select("policy_no")
    val ods_policy_detail_before = ods_policy_detail_r.filter("length(ent_id)>0").select("ent_id", "policy_code")
    val t1: RDD[(String, Int)] = employer_liability_claims.join(ods_policy_detail_before, employer_liability_claims("policy_no") === ods_policy_detail_before("policy_code")).map(x => {
      val policy_no = x.getString(0)
      val ent_id = x.getString(1)
      val policy_code = x.getString(2)
      (ent_id, (policy_code, policy_no))
    }).groupByKey.map(x => (x._1, x._2.size))

    //求出对应企业中有多少人交保了
    val ods_policy_insured_detail = ods_policy_insured_detail_r.select("policy_id")
    val ods_policy_detail = ods_policy_detail_r.select("policy_id", "ent_id", "policy_status")
    val t2: RDD[(String, Int)] = ods_policy_insured_detail.join(ods_policy_detail, "policy_id").where("policy_status in ('0','1', '7', '9', '10')").filter("length(ent_id)>0").map(x => (x.getString(1), (x.getString(0), x.getString(2)))).groupByKey.map(x => (x._1, x._2.size))
    //计算出险率：该企业出险的次数*100/该企业的投保人数
    val numberFormat = NumberFormat.getInstance
    numberFormat.setMaximumFractionDigits(4)
    val result = t1.join(t2).filter(_._1 != "").map(x => {
      //ent_id
      val ed: String = x._1
      val xq = x._2._1.toDouble //出险情的次数
      val tb = x._2._2.toDouble //该企业的投保人数
      (s"${get_hbase_key_name.getOrElse(ed, "null")}", xq.toString, tb.toString)
    }).toDF("ent_name", "xq", "tb")


    //根据企业名称进行join
    val end: RDD[(String, String, String)] = result.join(ods_ent_guzhu_salesman_channel.toDF("channel_name", "ent_name"), "ent_name").map(x => {
      val channel_name = x.getAs[String]("channel_name")
      val xq = x.getAs[String]("xq").toFloat
      val tb = x.getAs[String]("tb").toFloat
      (channel_name, (xq, tb))
    })
      //根据渠道名称进行分组，并将各个企业的男生人数，女生人数，总人数求和
      .reduceByKey((x1, x2) => {
      val xq = x1._1 + x2._1
      val tb = x1._2 + x2._2
      (xq, tb)
    }).map(x => {
      val channel_name_id = en_before.getOrElse(x._1, "null")
      val xq = x._2._1
      val tb = x._2._2
      val number = xq * 100 / tb
      (channel_name_id, number.toString, "ent_monthly_risk")
    })
    end
  }


  /**
    * 渠道材料完整度
    *
    * @param employer_liability_claims_r    出险表
    * @param ods_policy_detail_r            企业表
    * @param get_hbase_key_name             得到标签的key和企业明
    * @param sqlContext                     sqlContext
    * @param ods_ent_guzhu_salesman_channel 渠道表
    * @param en_before                      渠道名渠道ID
    * @return key value ent_material_integrity
    **/
  def ent_material_integrity(employer_liability_claims_r: DataFrame,
                             ods_policy_detail_r: DataFrame,
                             get_hbase_key_name: collection.Map[String, String],
                             sqlContext: HiveContext,
                             ods_ent_guzhu_salesman_channel: RDD[(String, String)],
                             en_before: collection.Map[String, String]
                            )
  : RDD[(String, String, String)]
  = {
    import sqlContext.implicits._
    val employer_liability_claims = employer_liability_claims_r.select("policy_no", "if_resubmit_paper")
    val ods_policy_detail = ods_policy_detail_r.select("ent_id", "policy_code")
    //通过保单号对理赔表，进行join，该张意外险所对应的，在保单表中的信息
    val tep_one = employer_liability_claims.join(ods_policy_detail, employer_liability_claims("policy_no") === ods_policy_detail("policy_code"))
    //计算我这个企业中有多少人发生了意外险（企业ID，该企业发生意外险的人数）
    val tep_two = tep_one.map(x => (x.getString(2), x.getString(0))).groupByKey.map(x => (x._1, x._2.size))
    //计算我这个企业中发生意外险的这张保单号，是否补过资料 (因为它补过资料所以是不完整的，最后的结果是用1-不完整度就是完整度了)
    val tep_three = tep_one.filter("if_resubmit_paper='是'").map(x => (x.getString(2), x.getString(1))).groupByKey.map(x => {
      //企业ID,保单号不过资料的总共有多少
      (x._1, x._2.size)
    })

    val end_result = tep_two.join(tep_three).filter(_._1 != "").map(f = x => {
      val ed = x._1 //ent_id
      val yw_sum = x._2._1 //意外险总人数
      val yw_b = x._2._2 //意外险补过资料的次数
      val yw_b_end = if (yw_b == "") 0 else yw_b
      //      (因为它补过资料所以是不完整的，最后的结果是用1-不完整度就是完整度了)

      (s"${get_hbase_key_name.getOrElse(ed, "null")}", yw_b_end.toString, yw_sum.toString)
    }).toDF("ent_name", "yw_b_end", "yw_sum")


    //根据企业名称进行join
    val end: RDD[(String, String, String)] = end_result.join(ods_ent_guzhu_salesman_channel.toDF("channel_name", "ent_name"), "ent_name").map(x => {
      val channel_name = x.getAs[String]("channel_name")
      val yw_b_end = x.getAs[String]("yw_b_end").toDouble
      val yw_sum = x.getAs[String]("yw_sum").toDouble
      (channel_name, (yw_b_end, yw_sum))
    })
      //根据渠道名称进行分组，并将各个企业的男生人数，女生人数，总人数求和
      .reduceByKey((x1, x2) => {
      val yw_b_end = x1._1 + x2._1
      val yw_sum = x1._2 + x2._2
      (yw_b_end, yw_sum)
    }).map(x => {
      val channel_name_id = en_before.getOrElse(x._1, "null")
      val yw_b_end = x._2._1
      val yw_sum = x._2._2
      val result = 1 - (yw_b_end / yw_sum)
      (channel_name_id, result.toString, "ent_material_integrity")
    })
    end

  }


  /**
    * 渠道平均赔付时效
    **/
  def avg_aging_cps(employer_liability_claims: DataFrame,
                    ods_policy_detail: DataFrame,
                    get_hbase_key_name: collection.Map[String, String],
                    sqlContext: HiveContext,
                    ods_ent_guzhu_salesman_channel: RDD[(String, String)],
                    en_before: collection.Map[String, String]
                   )
  : RDD[(String, String, String)]
  = {
    import sqlContext.implicits._
    //有中文则为true
    val tep_one_new = employer_liability_claims.filter("LENGTH(finish_days)<10 and LENGTH(finish_days)> 0").select("policy_no", "finish_days")
    val tep_two_new = ods_policy_detail.filter("LENGTH(ent_id)>0").select("policy_code", "ent_id")
    val tep_three = tep_one_new.join(tep_two_new, tep_one_new("policy_no") === tep_two_new("policy_code"))
    //通过police_no，找到该保单所对应的企业信息(join)
    //再根据企业ID进行分组，找出企业ID，保单号（结案的天数） 求个平均数(使用reduceByKey进行计算)
    //ent_id ,结案天数 （存到HBase中的avg_aging_cps
    val end_result = tep_three.map(x => {

      val finsh_days = x.getAs("finish_days").toString

      val p = Pattern.compile("[\u4e00-\u9fa5]")
      val m = p.matcher(finsh_days)
      val before_tepOne = m.find() //包含中文的话为0

      val finsh_days_end = if (before_tepOne) "0" else finsh_days //有中文则为0
      (x.getAs[String]("ent_id"), finsh_days_end)
    }).filter(!_._2.contains("=")).map(x => (x._1, (x._2.toDouble, 1))).reduceByKey((x1, x2) => {
      val day = x1._1 + x2._1
      val count = x1._2 + x2._2
      (day, count)
    }).map(x => (s"${get_hbase_key_name.getOrElse(x._1, "null")}", x._2._1.toString, x._2._2.toString)).toDF("ent_name", "day", "count")


    //根据企业名称进行join
    val end = end_result.join(ods_ent_guzhu_salesman_channel.toDF("channel_name", "ent_name"), "ent_name").map(x => {
      val channel_name = x.getAs[String]("channel_name")
      val day = x.getAs[String]("day").toDouble
      val count = x.getAs[String]("count").toDouble
      (channel_name, (day, count))
    })
      //根据渠道名称进行分组
      .reduceByKey((x1, x2) => {
      val day = x1._1 + x2._1
      val count = x1._2 + x2._2
      (day, count)
    })
      .map(x => {
        val channel_name_id = en_before.getOrElse(x._1, "null")
        val day = x._2._1
        val count = x._2._2
        val avg = day / count
        (channel_name_id, avg.toString, "avg_aging_cps")
      })
    end

  }


  /**
    * 渠道累加
    **/
  def channel_add(before: RDD[(String, String)],
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
        //企业，和企业对应的数据
        (end._1, before.getString("value"), before.getString("qual"))
      }).toDF("ent_name", "value", "qual")

    val end: RDD[(String, String, String)] = ods_ent_guzhu_salesman_channel.toDF("channel_name", "ent_name").join(ent_id, "ent_name")
      .map(x => (x.getAs[String]("channel_name"), x.getAs[String]("value").toDouble))
      .reduceByKey((x1, x2) => x1 + x2)
      .map(x => (en_before.getOrElse(x._1, "null"), x._2.toInt.toString, str))
    end
  }


  /**
    * 渠道平均出险周期
    **/
  def avg_aging_risk(ods_policy_detail: DataFrame,
                     ods_policy_risk_period: DataFrame,
                     get_hbase_key_name: collection.Map[String, String],
                     sqlContext: HiveContext,
                     ods_ent_guzhu_salesman_channel: RDD[(String, String)],
                     en_before: collection.Map[String, String]
                    )
  : RDD[(String, String, String)]
  = {

    import sqlContext.implicits._
    val tepOne = ods_policy_detail.select("ent_id", "policy_code")
    val tepTwo = ods_policy_risk_period.select("policy_no", "risk_period")
    val tepThree = tepOne.join(tepTwo, ods_policy_detail("policy_code") === ods_policy_risk_period("policy_no")).filter("length(ent_id)>0")
    val end_result = tepThree.map(x => (x.getString(0), (x.get(3).toString.toDouble, 1))).reduceByKey((x1, x2) => {
      val sum = x1._1 + x2._1
      val count = x1._2 + x2._2
      (sum, count)
    }).map(x =>
      (s"${get_hbase_key_name.getOrElse(x._1, "null")}", x._2._1.toString, x._2._2.toString)
    ).toDF("ent_name", "sum", "count")


    //根据企业名称进行join
    val end = end_result.join(ods_ent_guzhu_salesman_channel.toDF("channel_name", "ent_name"), "ent_name").map(x => {
      val channel_name = x.getAs[String]("channel_name")
      val sum = x.getAs[String]("sum").toDouble
      val count = x.getAs[String]("count").toDouble
      (channel_name, (sum, count))
    })
      //根据渠道名称进行分组
      .reduceByKey((x1, x2) => {
      val sum = x1._1 + x2._1
      val count = x1._2 + x2._2
      (sum, count)
    })
      .map(x => {
        val channel_name_id = en_before.getOrElse(x._1, "null")
        val sum = x._2._1
        val count = x._2._2
        val avg = sum / count
        (channel_name_id, avg.toString, "avg_aging_risk")
      })
    end
  }


  /**
    * 极短周期百分比(只取极短特征个数大于1的企业)
    **/
  def mix_period_rate(ods_policy_detail: DataFrame,
                      ods_policy_risk_period: DataFrame,
                      get_hbase_key_name: collection.Map[String, String],
                      sqlContext: HiveContext,
                      ods_ent_guzhu_salesman_channel: RDD[(String, String)],
                      en_before: collection.Map[String, String]
                     )
  : RDD[(String, String, String)]
  = {

    import sqlContext.implicits._
    val tepOne = ods_policy_detail.select("ent_id", "policy_code")
    val tepTwo = ods_policy_risk_period.select("policy_no", "risk_period")
    val tepThree = tepOne.join(tepTwo, ods_policy_detail("policy_code") === ods_policy_risk_period("policy_no")).filter("length(ent_id)>0")
    val total = tepThree.map(x => {
      val risk_period = x.get(3).toString.toDouble
      val ent_id = x.getString(0).toString
      (ent_id, risk_period)
    })
    //求出周期小于3的次数,并求出有多少个
    val threeNumber = total.reduceByKey((x1, x2) => {
      val before = if (x1 < 3) 1 else 0
      val after = if (x2 < 3) 1 else 0
      before + after
    }).map(x => (x._1, x._2))

    //总共有多少个
    val sum_Number = total.map(x => (x._1, 1)).reduceByKey(_ + _).map(x => (x._1, x._2))
    val end_result = threeNumber.join(sum_Number).map(x => (x._1, x._2._1, x._2._2)).filter(x => if (x._2 > 1) true else false).map(x => {
      //概率
      (s"${get_hbase_key_name.getOrElse(x._1, "null")}", x._2.toString, x._3.toString)
    }).toDF("ent_name", "before", "after")

    //根据企业名称进行join
    val end = end_result.join(ods_ent_guzhu_salesman_channel.toDF("channel_name", "ent_name"), "ent_name").map(x => {
      val channel_name = x.getAs[String]("channel_name")
      val before = x.getAs[String]("before").toDouble
      val after = x.getAs[String]("after").toDouble
      (channel_name, (before, after))
    })
      //根据渠道名称进行分组
      .reduceByKey((x1, x2) => {
      val before = x1._1 + x2._1
      val after = x1._2 + x2._2
      (before, after)
    })
      .map(x => {
        val channel_name_id = en_before.getOrElse(x._1, "null")
        val before = x._2._1
        val after = x._2._2
        val avg = before / after //概率
        (channel_name_id, avg.toString, "mix_period_rate")
      })
    end
  }


  /**
    * 渠道重大案件率
    **/
  def largecase_rate(ods_policy_detail: DataFrame,
                     employer_liability_claims: DataFrame,
                     get_hbase_key_name: collection.Map[String, String],
                     sqlContext: HiveContext,
                     ods_ent_guzhu_salesman_channel: RDD[(String, String)],
                     en_before: collection.Map[String, String]
                    )
  : RDD[(String, String, String)]
  = {

    import sqlContext.implicits._
    val tepOne = ods_policy_detail.select("ent_id", "policy_code")
    val tepTwo = employer_liability_claims.select("policy_no", "case_type")
    val tepThree = tepOne.join(tepTwo, ods_policy_detail("policy_code") === employer_liability_claims("policy_no")).filter("length(ent_id) > 0")
    val dath_number = tepThree.map(x => (x.getString(0), x.getString(3))).filter(x => if (x._2 == "死亡") true else false).map(x => (x._1, 1)).reduceByKey(_ + _).map(x => (x._1, x._2))
    //该企业发生残疾的人数
    val can_number = tepThree.map(x => (x.getString(0), x.getString(3))).filter(x => if (x._2 == "残疾") true else false).map(x => (x._1, 1)).reduceByKey(_ + _).map(x => (x._1, x._2))
    //该企业的总次数
    val total_number = tepThree.map(x => (x.getString(0), x.getString(3))).map(x => (x._1, 1)).reduceByKey(_ + _).map(x => x)

    val tepFour = dath_number.join(can_number).map(x => (x._1, (x._2._1, x._2._2))).filter(x => if (x._2._1 > 0 || x._2._2 > 0) true else false)
    val end_result = tepFour.join(total_number).map(x => {
      //企业ID，该企业死亡人数，该企业残疾人数,总人数
      val ent_id = x._1
      val deathNum = x._2._1._1.toDouble
      val disabilityNum = x._2._1._2.toDouble
      val totalNum = x._2._2

      (s"${get_hbase_key_name.getOrElse(ent_id, "null")}", s"${deathNum + disabilityNum}", totalNum.toString)

    }).toDF("ent_name", "before", "after")

    //根据企业名称进行join
    val end = end_result.join(ods_ent_guzhu_salesman_channel.toDF("channel_name", "ent_name"), "ent_name").map(x => {
      val channel_name = x.getAs[String]("channel_name")
      val before = x.getAs[String]("before").toDouble
      val after = x.getAs[String]("after").toDouble
      (channel_name, (before, after))
    })
      //根据渠道名称进行分组
      .reduceByKey((x1, x2) => {
      val before = x1._1 + x2._1
      val after = x1._2 + x2._2
      (before, after)
    })
      .map(x => {
        val channel_name_id = en_before.getOrElse(x._1, "null")
        val before = x._2._1
        val after = x._2._2
        val gailv = before / after
        (channel_name_id, gailv.toString, "largecase_rate")
      })
    end

  }

}
