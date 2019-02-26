package bzn.job.label.ent_claiminfo

import java.text.{NumberFormat, SimpleDateFormat}
import java.util.regex.Pattern
import java.util.{Date, Properties}

import bzn.job.common.Until
import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by a2589 on 2018/4/3.
  */
trait ClaiminfoUntilTest extends Until {

  //计算2个日期相隔多好天
  def xg(date3: String, date4: String): Int = {
    import java.text.SimpleDateFormat
    val format = new SimpleDateFormat("yyyy/MM/dd")
    val date1 = format.parse(date3)
    val date2 = format.parse(date4)

    val a = ((date1.getTime - date2.getTime) / (1000 * 3600 * 24)).asInstanceOf[Int]
    a.abs
  }

  //工种风险等级：对应的有多少人
  def ent_work_risk(ods_policy_insured_detail_r: DataFrame, ods_policy_detail_r: DataFrame, d_work_level_r: DataFrame): RDD[(String, String, String)] = {
    //ods_policy_insured_detail :被保人清单
    //ods_policy_detail：保单明细表
    //d_work_level:

    //work_type:工种类型|ai_level:等级
    //|         policy_id|   insured_cert_no|insured_work_type|insure_policy_status|
    //    |122008268007673856|44132219951005661X|              服务员|                   1|
    //policy_id,insured_cert_no,insured_work_type,insure_policy_status

    val ods_policy_insured_detail = ods_policy_insured_detail_r
      .select("policy_id", "insured_cert_no", "insured_work_type", "insure_policy_status")
    //policy_id,ent_id
    val ods_policy_detail = ods_policy_detail_r.select("policy_id", "ent_id")
    //work_type|ai_level

    val d_work_level = d_work_level_r.select("work_type", "ai_level")
    val ss = ods_policy_insured_detail.join(ods_policy_detail, "policy_id")
    //    |         policy_id|   insured_cert_no|insured_work_type|insure_policy_status|              ent_id|
    //    |122008268007673856|44132219951005661X|              服务员|                   1|10e11fd0b1a7488db...|
    val result = ss
      .join(d_work_level, ss("insured_work_type") === d_work_level("work_type"))
      .where("insure_policy_status='1' and ai_level is not null")
    //    |         policy_id|   insured_cert_no|insured_work_type|insure_policy_status|              ent_id|work_type|ai_level|
    //    |122008268007673856|44132219951005661X|              服务员|                   1|10e11fd0b1a7488db...|      服务员|       1|

    //企业ID，工种等级，该工种在该企业的人数(存到hbase中的worktype_工种等级_count)
    val end = result
      .map(x => {
        //      val policy_id = x.getString(0)
        val insured_cert_no = x.getString(1)
        //      val insured_work_type = x.getString(2)
        //      val insure_policy_status = x.getString(3)
        val ent_id = x.getString(4)
        //      val work_type = x.getString(5)
        val ai_level = x.get(6).toString
        ((ent_id, ai_level), insured_cert_no)
      })
      .groupByKey.map(x => {
      val e_Id: String = x._1._1
      val a_le: String = x._1._2
      val ccc = x._2.map(s => s).toSet.size
      //ent_id | 人数 | 等级
      //      (e_Id, ccc + "", s"worktype_${a_le}_count")
      (e_Id, ccc + "", a_le)
    })
    end
  }

  //员工增减行为(人数): 在同一个年单中超过2次减员的人的个数
  def ent_employee_increase(ods_policy_preserve_detail: DataFrame, ods_policy_detail: DataFrame): RDD[(String, String, String)] = {
    // ods_policy_preserve_detail:保全级别明细总表
    // 我们需求涉及到的就是：增减员和续投
    // insured_cert_no:证件号|insured_changetype:变化类型：1 增员；2 减员

    //计算我保全表中：这个人在企业的该保单中，减员超过2次的过滤出来
    val jy = ods_policy_preserve_detail
      .where("insured_changetype='2'")
      .select("policy_id", "insured_cert_no", "insured_changetype")
      .map(x => ((x.getString(0), x.getString(1)), x.getString(2)))
      .groupByKey
      .map(x => {
        //代表的是减员的次数
        val number = x._2.size
        // 这里解释一下：
        // 我在该保单中的这个人工人(证件编号)，有可能这个月在公司，下个月不再公司就不续保了就是减员，但是第二个月又来了就增员，循环....
        // 因此我计算的是：这个人在企业的该保单中，减员超过2次的过滤出来
        // police_id|insured_cert_no|代表的是减员的次数
        (x._1._1, (x._1._2, number))
      })
      .filter(_._2._2 > 2) //将大于2的次数过滤出去

    val bd_id = ods_policy_detail.select("policy_id", "ent_id")
      .map(x => (x.getString(0), x.getString(1)))

    //ent_id(rowKey),证件编号出现的次数（以“ent_monthly_risk”字段命名）
    //计算我从上面过滤出来的这个人，在保单表中找出其对应的企业ID,计算我这个企业中有多少个这样的人
    val result: RDD[(String, String, String)] = jy
      .join(bd_id)
      .map(x => {
        //ent_id
        val ent_id = x._2._2
        val insured_cert_no = x._2._1._1 //insured_cert_no(这个人在企业的该保单中，减员了超过2次)
        (ent_id, insured_cert_no)
      })
      .groupByKey.map(x => {
      val number_size = x._2.toSet.size
      //ent_id,证件编号出现的次数
      (x._1 + "", number_size + "", "ent_employee_increase")
    })
    result
  }

  //月均出现概率,每百人月均出险概率（逻辑改为:  每百人出险人数=总出险概率*100）
  def ent_monthly_risk(employer_liability_claims_r: DataFrame, ods_policy_detail_r: DataFrame,
                       ods_policy_insured_detail_r: DataFrame): RDD[(String, String, String)] = {

    //    ods_policy_detail	:保单详细表
    //    ods_policy_insured_detail :被保人详细表（一张保单可以对应多个被保人）
    //    employer_liability_claims :雇主保理赔记录表（在这张表里的都是出险了的，可以使用policy_no来计算）
    //            意思是：
    //              也可以理解为出险表，可以这样想，我生病了去看病，要报销，这就可以是一个数据

    //    policy_code：保单号
    //求出对应企业中有多少人出险了（怎么求出？我要计算出该出险保单所在的是哪个企业，使用的是join）
    val employer_liability_claims = employer_liability_claims_r.select("policy_no")
    val ods_policy_detail_before = ods_policy_detail_r.filter("length(ent_id)>0").select("ent_id", "policy_code")
    val t1: RDD[(String, Int)] = employer_liability_claims
      .join(ods_policy_detail_before, employer_liability_claims("policy_no") === ods_policy_detail_before("policy_code"))
      .map(x => {
        val policy_no = x.getString(0)
        val ent_id = x.getString(1)
        val policy_code = x.getString(2)
        (ent_id, (policy_code, policy_no))
      })
      .groupByKey
      // x._1 ent_id ，x._2.size
      .map(x => (x._1, x._2.size))

    //求出对应企业中有多少人交保了
    val ods_policy_insured_detail = ods_policy_insured_detail_r.select("policy_id")
    val ods_policy_detail = ods_policy_detail_r.select("policy_id", "ent_id", "policy_status")
    val t2: RDD[(String, Int)] = ods_policy_insured_detail
      .join(ods_policy_detail, "policy_id")
      .where("policy_status in ('0','1', '7', '9', '10')")
      .filter("length(ent_id)>0")
      .map(x => (x.getString(1), (x.getString(0), x.getString(2))))
      .groupByKey
      // x._1 ent_id,  x._2.size 计算次数
      .map(x => (x._1, x._2.size))

    //计算出险率：该企业出险的次数*100/该企业的投保人数
    val numberFormat = NumberFormat.getInstance
    numberFormat.setMaximumFractionDigits(4)
    //企业ID(rowKey),出险率(存到了Hbase中的ent_monthly_risk)
    val result: RDD[(String, String, String)] = t1
      .join(t2)
      .filter(_._1 != "")
      .map(x => {
        //ent_id
        val ed: String = x._1
        //出险情的次数
        val xq = x._2._1.toDouble
        //该企业的投保人数
        val tb = x._2._2.toDouble
        val number = xq * 100 / tb
        val res = numberFormat.format(number)
        (ed, number.toString, "ent_monthly_risk")
      })
    result
  }

  //材料完整度
  def ent_material_integrity(employer_liability_claims_r: DataFrame,
                             ods_policy_detail_r: DataFrame): RDD[(String, String, String)] = {

    //employer_liability_claims	雇主保理赔记录表（是通过policy_no进行记的，因为报案这张表是手动进行等级的因此，它没办法记住ID）
    val employer_liability_claims = employer_liability_claims_r.select("policy_no", "if_resubmit_paper")
    //    val employer_liability_claims = sqlContext.sql("select lc.policy_no,lc.if_resubmit_paper from odsdb_prd.employer_liability_claims lc")
    val ods_policy_detail = ods_policy_detail_r.select("ent_id", "policy_code")
    //    val ods_policy_detail = sqlContext.sql("select pd.ent_id ,pd.policy_code from odsdb_prd.ods_policy_detail pd")

    //通过保单号对理赔表，进行join，该张意外险所对应的，在保单表中的信息
    val tep_one = employer_liability_claims
      .join(ods_policy_detail, employer_liability_claims("policy_no") === ods_policy_detail("policy_code"))
    //      .show()
    //    |         policy_no|if_resubmit_paper|              ent_id|       policy_code|
    //    |HL1100000029006495|                 |f3f4c2611e3b43fea...|HL1100000029006495|

    //计算我这个企业中有多少人发生了意外险（企业ID，该企业发生意外险的人数）
    val tep_two = tep_one
      //ent_id | policy_no
      .map(x => (x.getString(2), x.getString(0)))
      .groupByKey
      .map(x => (x._1, x._2.size))
    //计算我这个企业中发生意外险的这张保单号，是否补过资料 (因为它补过资料所以是不完整的，最后的结果是用1-不完整度就是完整度了)
    val tep_three = tep_one
      .filter("if_resubmit_paper='是'")
      //ent_id      |   保单号
      .map(x => (x.getString(2), x.getString(1)))
      .groupByKey
      //企业ID,保单号不过资料的总共有多少
      .map(x => (x._1, x._2.size))

    val numberFormat = NumberFormat.getInstance
    numberFormat.setMaximumFractionDigits(4)

    //将tep_two与tep_three想join,得到企业ID所对应的意外险人数和补过资料的次数
    //企业ID,完整度的概率(存到了hbase中的ent_material_integrity)
    val end_result: RDD[(String, String, String)] = tep_two
      .join(tep_three)
      .filter(_._1 != "")
      .map(f = x => {
        //ent_id
        val ed = x._1
        //意外险总人数
        val yw_sum = x._2._1
        //意外险补过资料的次数
        val yw_b = x._2._2

        val yw_b_end = if (yw_b == "") 0 else yw_b
        // 因为它补过资料所以是不完整的，最后的结果是用1-不完整度就是完整度了
        val result = 1 - (yw_b_end.toDouble / yw_sum.toDouble)
        // ed, numberFormat.format(result)
        (ed, result.toString)
      })
      .map(x => (x._1, x._2, "ent_material_integrity"))

    end_result
  }

  //平均赔付时效
  def avg_aging_cps(employer_liability_claims: DataFrame, ods_policy_detail: DataFrame): RDD[(String, String, String)] = {
    //有中文则为true
    val numberFormat = NumberFormat.getInstance
    numberFormat.setMaximumFractionDigits(2)

    //case_close_date:结案日期
    //paper_finish_date:材料补充完成日期（比如我生病了: 会出事一些病例之类的，这些都是资料）
    //finish_days:结案时效 (材料补充完成日期，到我结案了，隔了多少天)
    val tep_one_new = employer_liability_claims
      .filter("LENGTH(finish_days)<10 and LENGTH(finish_days)> 0")
      .select("policy_no", "finish_days")
    val tep_two_new = ods_policy_detail
      .filter("LENGTH(ent_id)>0")
      .select("policy_code", "ent_id")
    val tep_three = tep_one_new.join(tep_two_new, tep_one_new("policy_no") === tep_two_new("policy_code"))
    //          .show()
    //    |         policy_no|finish_days|       policy_code|              ent_id|
    //    +------------------+-----------+------------------+--------------------+
    //    |900000046998381950|      23.00|900000046998381950|ddc05b821562470fa...|
    //通过police_no，找到该保单所对应的企业信息(join)
    //再根据企业ID进行分组，找出企业ID，保单号（结案的天数） 求个平均数(使用reduceByKey进行计算)
    //ent_id ,结案天数 （存到HBase中的avg_aging_cps
    val ens: RDD[(String, String, String)] = tep_three
      .map(x => {
        val finsh_days = x.getAs("finish_days").toString
        val p = Pattern.compile("[\u4e00-\u9fa5]")
        val m = p.matcher(finsh_days)
        //包含中文的话为0
        val before_tepOne = m.find()
        //有中文则为0
        val finsh_days_end = if (before_tepOne) "0" else finsh_days
        (x.getAs[String]("ent_id"), finsh_days_end)
      })
      .filter(!_._2.contains("="))
      .map(x => (x._1, (x._2.toDouble, 1)))
      .reduceByKey((x1, x2) => {
        val day = x1._1 + x2._1
        val count = x1._2 + x2._2
        (day, count)
      })
      .map(x => {
        val s = x._2._1 / x._2._2
        (x._1, s.toString, "avg_aging_cps")
      })
    ens
  }

  //企业报案件数(雇主理赔表与保单表join，就可以知道有哪些企业发生了雇主理赔，同时统计该企业的次数)
  def report_num(employer_liability_claims: DataFrame, ods_policy_detail: DataFrame): RDD[(String, String, String)] = {
    val tepOne = ods_policy_detail.select("ent_id", "policy_code").filter("LENGTH(ent_id)>0")
    val tepTwo = employer_liability_claims.select("policy_no")
    val tepThree = tepOne.join(tepTwo, ods_policy_detail("policy_code") === employer_liability_claims("policy_no"))
    //    |              ent_id|         policy_code|           policy_no|
    //    |0a789d56b7444d519...|  900000047702719243|  900000047702719243|
    //end_id|该企业报案的次数 (存到Hbase中的report_num字段)
    //ent_id
    val end = tepThree
      .map(x => (x.getString(0), 1))
      .reduceByKey((x1, x2) => {
        val count = x1 + x2
        count
      })
      .map(x => (x._1, x._2 + "", "report_num"))

    end
  }

  // 企业理赔件数
  // 雇主理赔表与保单表join，就可以知道有哪些企业发生了雇主理赔，
  // 如果再加一个条件（paper_finish_date:材料补充完成日期）将其 > 0的过滤出来，
  // 就表示材料补充完了，该理赔了，同时统计该企业理赔的次数
  def claim_num(employer_liability_claims: DataFrame, ods_policy_detail: DataFrame): RDD[(String, String, String)] = {
    val tepOne = ods_policy_detail.select("ent_id", "policy_code").filter("LENGTH(ent_id)>0")
    val tepTwo = employer_liability_claims.select("policy_no", "paper_finish_date")
    val tepThree = tepOne.join(tepTwo, ods_policy_detail("policy_code") === employer_liability_claims("policy_no")).filter("LENGTH(paper_finish_date)>0")
    //    |              ent_id|       policy_code|         policy_no|paper_finish_date|
    //    |ddc05b821562470fa...|HL1100000096000201|HL1100000096000201|        2017/8/21|
    //end_id | 该企业理赔件数的次数(存到HBase中的claim_num字段中)
    val end = tepThree
      .map(x => (x.getString(0), 1))
      .reduceByKey(_ + _)
      .map(x => (x._1, x._2 + "", "claim_num"))

    end
  }

  //死亡案件统计
  def death_num(employer_liability_claims: DataFrame, ods_policy_detail: DataFrame): RDD[(String, String, String)] = {
    val tepOne = ods_policy_detail.select("ent_id", "policy_code")
    val tepTwo = employer_liability_claims.select("policy_no", "case_type")
    val tepThree = tepOne.join(tepTwo, ods_policy_detail("policy_code") === employer_liability_claims("policy_no"))
      .filter("LENGTH(ent_id)>0").filter("case_type='死亡'")
    //      .show()
    //    |              ent_id|       policy_code|         policy_no|case_type|
    //    |f3f4c2611e3b43fea...|HL1100000029006495|HL1100000029006495|       死亡|
    //ent_id | 计算出现的次数(存到HBase的death_num字段中)
    val end = tepThree
      .map(x => (x.getString(0), 1))
      .reduceByKey(_ + _).map(x => (x._1, x._2 + "", "death_num"))

    end
  }

  //伤残案件数
  def disability_num(employer_liability_claims: DataFrame, ods_policy_detail: DataFrame): RDD[(String, String, String)] = {
    val tepOne = ods_policy_detail.select("ent_id", "policy_code")
    val tepTwo = employer_liability_claims.select("policy_no", "case_type")
    val tepThree = tepOne
      .join(tepTwo, ods_policy_detail("policy_code") === employer_liability_claims("policy_no"))
      .filter("LENGTH(ent_id)>0")
      .filter("case_type='残疾'")
    //    |              ent_id|       policy_code|         policy_no|case_type|
    //    |f3f4c2611e3b43fea...|HL1100000029006495|HL1100000029006495|       残疾|
    //ent_id | 计算出现的次数(存到HBase的disability_num字段中)
    val end = tepThree
      .map(x => (x.getString(0), 1))
      .reduceByKey(_ + _)
      .map(x => (x._1, x._2 + "", "disability_num"))

    end
  }

  //工作期间案件数
  def worktime_num(employer_liability_claims: DataFrame, ods_policy_detail: DataFrame): RDD[(String, String, String)] = {
    //scene：发生场景
    val tepOne = ods_policy_detail.select("ent_id", "policy_code")
    val tepTwo = employer_liability_claims.select("policy_no", "scene")
    val tepThree = tepOne
      .join(tepTwo, ods_policy_detail("policy_code") === employer_liability_claims("policy_no"))
      .filter("LENGTH(ent_id)>0")
      .filter("scene='工作期间'")
    //    |              ent_id|         policy_code|           policy_no|scene|
    //    |0a789d56b7444d519...|  900000047702719243|  900000047702719243| 工作期间|
    //ent_id | 计算出现的次数(存到HBase的worktime_num字段中)
    val end = tepThree
      .map(x => (x.getString(0), 1))
      .reduceByKey(_ + _)
      .map(x => (x._1, x._2 + "", "worktime_num"))

    end
  }

  //非工作期间案件数
  def nonworktime_num(employer_liability_claims: DataFrame, ods_policy_detail: DataFrame): RDD[(String, String, String)] = {
    //scene：发生场景
    val tepOne = ods_policy_detail.select("ent_id", "policy_code")
    val tepTwo = employer_liability_claims.select("policy_no", "scene")
    val tepThree = tepOne
      .join(tepTwo, ods_policy_detail("policy_code") === employer_liability_claims("policy_no"))
      .filter("LENGTH(ent_id)>0")
      .where("scene in ('非工作期间', '上下班') ")
    //    |              ent_id|       policy_code|         policy_no|scene|
    //    |6a2a4cb48d5b464d9...|HL1100000080001229|HL1100000080001229|  上下班|
    //ent_id | 计算出现的次数(存到HBase的nonworktime_num字段中)
    val end = tepThree.map(x => (x.getString(0), 1))
      .reduceByKey(_ + _)
      .map(x => (x._1, x._2 + "", "nonworktime_num"))

    end
  }

  //预估总赔付金额
  def pre_all_compensation(ods_ent_guzhu_salesman:RDD[(String, String)],sqlContext:HiveContext,bro_dim: Broadcast[Array[String]],employer_liability_claims: DataFrame, ods_policy_detail: DataFrame): RDD[(String, String, String)] = {
    //final_payment：最终赔付金额
    //pre_com：预估赔付金额
    import sqlContext.implicits._
    val numberFormat = NumberFormat.getInstance
    numberFormat.setMaximumFractionDigits(2)
    var ods_ent_guzhu_salesman_temp = ods_ent_guzhu_salesman.toDF("ent_name","channel_name")
    //      .filter("channel_name = '重庆翔耀保险咨询服务有限公司'")
    val tep_temp = ods_policy_detail.map(x => (x.getAs[String]("insure_code"), x)).filter(x => if (bro_dim.value.contains(x._1)) true else false)
      .map(x => {
        (x._2.getAs[String]("ent_id"), x._2.getAs[String]("policy_code"),x._2.getAs[String]("holder_company"))
      }).toDF("ent_id", "policy_code","holder_company").filter("ent_id is not null").cache
    val tepOne = tep_temp.join(ods_ent_guzhu_salesman_temp, tep_temp("holder_company") === ods_ent_guzhu_salesman_temp("ent_name"))
      .map(x => {
        (x.getAs[String]("ent_id").toString,x.getAs[String]("policy_code").toString)
      }).toDF("ent_id","policy_code")

    val tepTwo = employer_liability_claims.select("policy_no", "final_payment", "pre_com").map(x => {
      var policy_no = x.getAs[String]("policy_no").trim
      var final_payment = x.getAs[String]("final_payment")
      var pre_com = x.getAs[String]("pre_com")
      (policy_no,final_payment,pre_com)
    }).toDF("policy_no","final_payment","pre_com")
    val tepThree = tepOne.join(tepTwo, tepOne("policy_code") === tepTwo("policy_no"),"left").filter("LENGTH(ent_id)>0")
    //      .show()
    //    |              ent_id|         policy_code|           policy_no|final_payment|pre_com|
    //    |0a789d56b7444d519...|  900000047702719243|  900000047702719243|             |   5000|

    //end_id | 金额总值(存到HBase中的pre_all_compensation)
    val end: RDD[(String, String, String)] = tepThree.map(x => x)
      .filter(x => {
        var str = ""
        if(x.get(4) != null){
          str = x.get(4).toString
        }
        val p = Pattern.compile("[\u4e00-\u9fa5]")
        val m = p.matcher(str)
        if (!m.find) true else false
      })
      .map(x => {
        val final_payment = x.getAs[String]("final_payment")
        val pre_com = x.getAs[String]("pre_com")
        //如果最终赔付金额有值，就取最终金额，否则就取预估金额
        val res = if (final_payment == "" || final_payment == null) pre_com else if (final_payment != "" || final_payment != null) final_payment else ""
        (x.getString(0), res)
      })
      .filter(x => x._2 != "").filter(_._2 != ".").filter(_._2 != "#N/A").filter(x =>x._2 != null)
      .map(x => (x._1, x._2.toDouble))
      .reduceByKey(_ + _).map(x => {
      //      (x._1, numberFormat.format(x._2), "pre_all_compensation")
      (x._1, x._2.toString, "pre_all_compensation")
    })
    end
  }

  //死亡预估配额
  def pre_death_compensation(employer_liability_claims: DataFrame, ods_policy_detail: DataFrame): RDD[(String, String, String)] = {
    //final_payment：最终赔付金额
    //pre_com：预估赔付金额
    //case_type:伤情类别
    val numberFormat = NumberFormat.getInstance
    numberFormat.setMaximumFractionDigits(2)

    val tepOne = ods_policy_detail.select("ent_id", "policy_code")
    val tepTwo = employer_liability_claims.select("policy_no", "final_payment", "pre_com", "case_type")
    val tepThree = tepOne
      .join(tepTwo, ods_policy_detail("policy_code") === employer_liability_claims("policy_no"))
      .filter("LENGTH(ent_id)>0")
      .filter("case_type='死亡'")
    //    |              ent_id|       policy_code|         policy_no|final_payment|pre_com|case_type|
    //    |f3f4c2611e3b43fea...|HL1100000029006495|HL1100000029006495|             | 800000|       死亡|
    //end_id | 金额总值(存到HBase中的pre_death_compensation)
    val end: RDD[(String, String, String)] = tepThree
      .map(x => x)
      .filter(x => {
        val str = x.get(4).toString
        val p = Pattern.compile("[\u4e00-\u9fa5]")
        val m = p.matcher(str)
        if (!m.find) true else false
      })
      .map(x => {
        val final_payment = x.getAs[String]("final_payment")
        val pre_com = x.getAs[String]("pre_com")
        //如果最终赔付金额有值，就取最终金额，否则就取预估金额
        val res = if (final_payment == "" || final_payment == null) pre_com else if (final_payment != "" || final_payment != null) final_payment else ""
        (x.getString(0), res)
      })
      .filter(x => x._2 != "")
      .filter(_._2 != ".")
      .filter(_._2 != "#N/A")
      .map(x => (x._1, x._2.toDouble))
      .reduceByKey(_ + _)
      // (x._1, numberFormat.format(x._2), "pre_death_compensation")
      .map(x => (x._1, x._2.toString, "pre_death_compensation"))

    end
  }

  //伤残预估配额
  def pre_dis_compensation(employer_liability_claims: DataFrame, ods_policy_detail: DataFrame): RDD[(String, String, String)] = {
    //disable_level：伤残 "等级"
    val numberFormat = NumberFormat.getInstance
    numberFormat.setMaximumFractionDigits(2)

    val tepOne = ods_policy_detail.select("ent_id", "policy_code")
    val tepTwo = employer_liability_claims
      .select("policy_no", "final_payment", "pre_com", "disable_level")
      .filter("length(final_payment)> 0 or length(pre_com) >0")
    val tepThree = tepOne
      .join(tepTwo, ods_policy_detail("policy_code") === employer_liability_claims("policy_no"))
      .filter("LENGTH(ent_id)>0")
      .where("disable_level not in ('无','死亡')")
    //    |              ent_id|       policy_code|         policy_no|final_payment|pre_com|disable_level|
    //    |a1c4f6ede9b14f05b...|HL1100000067004072|HL1100000067004072|             |  30000|          7-9|
    // end_id | 金额总值(存到HBase中的pre_dis_compensation)
    val end: RDD[(String, String, String)] = tepThree
      .map(x => x)
      .filter(x => {
        val str = x.get(4).toString
        val p = Pattern.compile("[\u4e00-\u9fa5]")
        val m = p.matcher(str)
        if (!m.find) true else false
      })
      .map(x => {
        val final_payment = x.getAs[String]("final_payment")
        val pre_com = x.getAs[String]("pre_com")
        //如果最终赔付金额有值，就取最终金额，否则就取预估金额
        val res = if (final_payment == "" || final_payment == null) pre_com else if (final_payment != "" || final_payment != null) final_payment else ""
        (x.getString(0), res)
      })
      .filter(x => x._2 != "").filter(_._2 != ".").filter(_._2 != "#N/A")
      .map(x => (x._1, x._2.toDouble))
      .reduceByKey(_ + _)
      // (x._1, numberFormat.format(x._2), "pre_dis_compensation")
      .map(x => (x._1, x._2.toString, "pre_dis_compensation"))
    end
  }

  //工作期间预估赔付
  def pre_wt_compensation(employer_liability_claims: DataFrame, ods_policy_detail: DataFrame): RDD[(String, String, String)] = {
    //scene：发生场景
    val numberFormat = NumberFormat.getInstance
    numberFormat.setMaximumFractionDigits(2)

    val tepOne = ods_policy_detail.select("ent_id", "policy_code")
    val tepTwo = employer_liability_claims.select("policy_no", "final_payment", "pre_com", "scene")
    val tepThree = tepOne
      .join(tepTwo, ods_policy_detail("policy_code") === employer_liability_claims("policy_no"))
      .filter("LENGTH(ent_id)>0")
      .filter("scene='工作期间'")
    //      .show()
    //    |              ent_id|         policy_code|           policy_no|final_payment|pre_com|scene|
    //    |0a789d56b7444d519...|  900000047702719243|  900000047702719243|             |   5000| 工作期间|
    //end_id | 金额总值(存到HBase中的pre_wt_compensation)
    val end = tepThree
      .map(x => x)
      .filter(x => {
        val str = x.get(4).toString
        val p = Pattern.compile("[\u4e00-\u9fa5]")
        val m = p.matcher(str)
        if (!m.find) true else false
      })
      .map(x => {
        val final_payment = x.getAs[String]("final_payment")
        val pre_com = x.getAs[String]("pre_com")
        //如果最终赔付金额有值，就取最终金额，否则就取预估金额
        val res = if (final_payment == "" || final_payment == null) pre_com else if (final_payment != "" || final_payment != null) final_payment else ""
        (x.getString(0), res)
      })
      .filter(x => x._2 != "")
      .filter(_._2 != ".")
      .filter(_._2 != "#N/A")
      .map(x => (x._1, x._2.toDouble))
      .reduceByKey(_ + _)
      //      (x._1, numberFormat.format(x._2), "pre_wt_compensation")
      .map(x => (x._1, x._2.toString, "pre_wt_compensation"))

    end
  }

  //非工作期间预估赔付
  def pre_nwt_compensation(employer_liability_claims: DataFrame, ods_policy_detail: DataFrame): RDD[(String, String, String)] = {
    val numberFormat = NumberFormat.getInstance
    numberFormat.setMaximumFractionDigits(2)

    val tepOne = ods_policy_detail.select("ent_id", "policy_code")
    val tepTwo = employer_liability_claims.select("policy_no", "final_payment", "pre_com", "scene")
    val tepThree = tepOne.join(tepTwo, ods_policy_detail("policy_code") === employer_liability_claims("policy_no"))
      .filter("LENGTH(ent_id)>0").where("scene in ('非工作期间', '上下班')")
    //      .show()
    //    |              ent_id|         policy_code|           policy_no|final_payment|pre_com|scene|
    //    |0a789d56b7444d519...|  900000047702719243|  900000047702719243|             |   5000| 上下班|
    //end_id | 金额总值(存到HBase中的pre_nwt_compensation)
    val end: RDD[(String, String, String)] = tepThree
      .map(x => x)
      .filter(x => {
        val str = x.get(4).toString
        val p = Pattern.compile("[\u4e00-\u9fa5]")
        val m = p.matcher(str)
        if (!m.find) true else false
      })
      .map(x => {
        val final_payment = x.getAs[String]("final_payment")
        val pre_com = x.getAs[String]("pre_com")
        //如果最终赔付金额有值，就取最终金额，否则就取预估金额
        val res = if (final_payment == "" || final_payment == null) pre_com else if (final_payment != "" || final_payment != null) final_payment else ""
        (x.getString(0), res)
      })
      .filter(x => x._2 != "")
      .filter(_._2 != ".")
      .filter(_._2 != "#N/A")
      .map(x => (x._1, x._2.toDouble))
      //      (x._1, numberFormat.format(x._2), "pre_nwt_compensation")
      .reduceByKey(_ + _)
      .map(x => (x._1, x._2.toString, "pre_nwt_compensation"))

    end
  }

  //实际已赔付金额
  def all_compensation(employer_liability_claims: DataFrame, ods_policy_detail: DataFrame): RDD[(String, String, String)] = {
    val numberFormat = NumberFormat.getInstance
    numberFormat.setMaximumFractionDigits(2)

    val tepOne = ods_policy_detail.select("ent_id", "policy_code")
    val tepTwo = employer_liability_claims.select("policy_no", "final_payment", "pre_com", "scene")
    val tepThree = tepOne.join(tepTwo, ods_policy_detail("policy_code") === employer_liability_claims("policy_no"))
      .filter("LENGTH(ent_id)>0").filter("LENGTH(final_payment)>0")
    //      .show()
    //    |              ent_id|       policy_code|         policy_no|final_payment|pre_com|scene|
    //    |ddc05b821562470fa...|HL1100000096000201|HL1100000096000201|       539.83|   1000| 工作期间|
    //end_id | 金额总值(存到HBase中的all_compensation)
    val end = tepThree
      .map(x => {
        val final_payment = x.getString(3)
        (x.getString(0), final_payment.toDouble)
      })
      .reduceByKey(_ + _)
      // (x._1, numberFormat.format(x._2), "all_compensation")
      .map(x =>
      (x._1, x._2.toString, "all_compensation"))

    end
  }

  //超时赔付案件数
  def overtime_compensation(employer_liability_claims: DataFrame, ods_policy_detail: DataFrame): RDD[(String, String, String)] = {
    //id:是该表的主键
    val tepOne = ods_policy_detail.select("ent_id", "policy_code")
    val tepTwo = employer_liability_claims.select("policy_no", "overtime", "id")
    val tepThree = tepOne.join(tepTwo, ods_policy_detail("policy_code") === employer_liability_claims("policy_no"))
      .filter("LENGTH(ent_id)>0").filter("overtime='是'")
    //    |              ent_id|       policy_code|         policy_no|overtime|   id|
    //    |365104828040476c9...|HL1100000176000538|HL1100000176000538|       是|77053|
    val end = tepThree
      //ent_id | id
      .map(x => (x.getString(0), x.getString(4)))
      .reduceByKey((x1, x2) => {
        //通过reduceByKey对Key进行分组，然后去重
        val sum = x1 + "\t" + x2
        sum
      }).map(x => {
      val count = x._2.split("\t").toSet.size
      (x._1, count + "", "overtime_compensation")
    })

    end
  }

  //已赚保费
  def charged_premium(ods_policy_detail: DataFrame, bro_dim: Broadcast[Array[String]], ods_policy_insured_charged: DataFrame): RDD[(String, String, String)] = {
    //premium:保费
    //sku_charge_type:特约(1:不含实习生---2:包含实习生)
    //insure_code：险种|产品代码
    //sku_day_price:每日的保费
    //policy_status:保单状态，主要对应留学保保单状态(1：有效；0：无效；2：未完成；3：核保失败；4：预售成功；5：待支付；6：承保中；7：已承保；8：已取消；9：已终止; 10：已过期)
    val numberFormat = NumberFormat.getInstance
    numberFormat.setMaximumFractionDigits(2)
    val qt_data = ods_policy_detail
      .where("sku_charge_type!='2'")
      .where("policy_status in ('0','1','7','9','10')")
      .select("ent_id", "premium", "insure_code", "policy_id")
      .map(x => (x.getString(0), x.get(1).toString, x.get(2).toString, x.get(3).toString))
      .filter(x => if (bro_dim.value.contains(x._3)) true else false)
      .filter(_._2 != "#N/A")
    //求出保单表中的，保费.该保费是每月该企业所要缴纳的保费，每个月缴纳保费一次都会产生一个保单号
    //ent_id | 保费
    val bf_sum: RDD[(String, Double)] = qt_data.map(x => (x._1, x._2.toDouble))
    //ods_policy_insured_charged:该表包含 保单ID，日期(也就是起保日期)，每天的保费 | 我们要将
    val currentTime = System.currentTimeMillis
    val simpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val tepThree = ods_policy_detail.join(ods_policy_insured_charged, "policy_id")
    val end: RDD[(String, String, String)] = tepThree
      .select("ent_id", "insure_code", "sku_charge_type", "policy_status", "day_id", "sku_day_price")
      .where("sku_charge_type='2' and policy_status in ('0','1','7','9','10')")
      .filter("LENGTH(ent_id)>0")
      .map(x => {
        val ent_id = x.getString(0)
        val insure_code = x.get(1) + ""
        val day_id = x.get(4) + ""
        val sku_day_price = x.get(5) + ""
        (ent_id, insure_code, day_id, sku_day_price)
      })
      .filter(x => if (bro_dim.value.contains(x._2)) true else false)
      .filter(x => {
        val date = simpleDateFormat.parse(x._3)
        val ts = date.getTime
        if (ts < currentTime) true else false
      })
      // ent_id | 没人每天的保费
      .map(x => (x._1, x._4.toDouble))
      .union(bf_sum)
      .reduceByKey(_ + _)
      // (x._1, numberFormat.format(x._2), "charged_premium")
      .map(x => (x._1, x._2.toString, "charged_premium"))

    end
  }

  //已赚保费新
  def charged_premium_new(sqlContext: HiveContext, location_mysql_url: String, prop: Properties,
                          ods_policy_detail: DataFrame): RDD[(String, String, String)] = {
    import sqlContext.implicits._

    val ods_policy_charged_month = sqlContext.read.jdbc(location_mysql_url, "ods_policy_charged_month", prop)
      .mapPartitions(par => {
        val now: Date = new Date()
        val dateFormatOne: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
        val now_Date = dateFormatOne.format(now)
        par.map(x => {
          (x.getAs[String]("policy_id"), x.getAs[java.math.BigDecimal]("charged_premium"), x.getAs[String]("day_id"))
        }).filter(_._3.toDouble <= now_Date.toDouble)
      })
      .toDF("policy_id", "charged_premium", "day_id")

    val end: RDD[(String, String, String)] = ods_policy_charged_month
      .join(ods_policy_detail, "policy_id")
      .map(x => {
        val ent_id = x.getAs[String]("ent_id")
        val charged_premium = x.getAs[java.math.BigDecimal]("charged_premium").toString.toDouble
        (ent_id, charged_premium)
      })
      .reduceByKey((x1, x2) => x1 + x2).map(x => (x._1, x._2 + "", "charged_premium"))

    end
  }

  //企业拒赔次数我
  def ent_rejected_count(employer_liability_claims: DataFrame, ods_policy_detail: DataFrame): RDD[(String, String, String)] = {
    val tepOne = ods_policy_detail.select("ent_id", "policy_code")
    val tepTwo = employer_liability_claims.select("policy_no", "status", "id")
    val tepThree = tepOne
      .join(tepTwo, ods_policy_detail("policy_code") === employer_liability_claims("policy_no"))
      .filter("length(ent_id)>0")
      .filter("status='拒赔'")
    //    |              ent_id|       policy_code|         policy_no|status|   id|
    //    |365104828040476c9...|HL1100000064004245|HL1100000064004245|    拒赔|77222|
    val end: RDD[(String, String, String)] = tepThree
      .map(x => (x.getString(0), x.getString(4)))
      .reduceByKey((x1, x2) => {
        val sum = x1 + "\t" + x2
        sum
      })
      .map(x => {
        val number = x._2.split("\t").toSet.size
        (x._1, number + "", "ent_rejected_count")
      })

    end
  }

  //企业撤案次数
  def ent_withdrawn_count(employer_liability_claims: DataFrame, ods_policy_detail: DataFrame): RDD[(String, String, String)] = {
    val tepOne = ods_policy_detail.select("ent_id", "policy_code")
    val tepTwo = employer_liability_claims.select("policy_no", "status", "id")
    val tepThree = tepOne.join(tepTwo, ods_policy_detail("policy_code") === employer_liability_claims("policy_no"))
      .filter("length(ent_id)>0").filter("status='撤案'")
    //    |              ent_id|       policy_code|         policy_no|status|   id|
    //    |365104828040476c9...|HL1100000064004245|HL1100000064004245|    撤案|77222|
    val end: RDD[(String, String, String)] = tepThree
      .map(x => (x.getString(0), x.getString(4)))
      .reduceByKey((x1, x2) => {
        val sum = x1 + "\t" + x2
        sum
      })
      .map(x => {
        val number = x._2.split("\t").toSet.size
        (x._1, number + "", "ent_withdrawn_count")
      })
    end
  }

  //平均出险周期
  def avg_aging_risk(ods_policy_detail: DataFrame, ods_policy_risk_period: DataFrame): RDD[(String, String, String)] = {
    val numberFormat = NumberFormat.getInstance
    numberFormat.setMaximumFractionDigits(2)

    val tepOne = ods_policy_detail.select("ent_id", "policy_code")
    val tepTwo = ods_policy_risk_period.select("policy_no", "risk_period")
    val tepThree = tepOne.join(tepTwo, ods_policy_detail("policy_code") === ods_policy_risk_period("policy_no"))
      .filter("length(ent_id)>0")
    //    |              ent_id|         policy_code|           policy_no|risk_period|
    //    |0a789d56b7444d519...|  900000047702719243|  900000047702719243|         57|
    //end_id | risk_period的平均数
    val end: RDD[(String, String, String)] = tepThree
      .map(x => (x.getString(0), (x.get(3).toString.toDouble, 1)))
      .reduceByKey((x1, x2) => {
        val sum = x1._1 + x2._1
        val count = x1._2 + x2._2
        (sum, count)
      })
      .map(x => {
        val avg = x._2._1 / x._2._2
        // (x._1, numberFormat.format(avg), "avg_aging_risk")
        (x._1, avg.toString, "avg_aging_risk")
      })

    end
  }

  //极短周期特征（出险周期小于3的案件数）
  def mix_period_count(ods_policy_detail: DataFrame, ods_policy_risk_period: DataFrame): RDD[(String, String, String)] = {
    val tepOne = ods_policy_detail.select("ent_id", "policy_code")
    val tepTwo = ods_policy_risk_period.select("policy_no", "risk_period")
    val tepThree = tepOne.join(tepTwo, ods_policy_detail("policy_code") === ods_policy_risk_period("policy_no"))
      .filter("length(ent_id)>0").where("risk_period < 3")
    //      .show()
    //    |              ent_id|         policy_code|           policy_no|risk_period|
    //    |0a789d56b7444d519...|  900000047702719243|  900000047702719243|         57|
    //end_id | 出现的次数
    val end: RDD[(String, String, String)] = tepThree
      .map(x => (x.getString(0), 1))
      .reduceByKey(_ + _)
      .map(x => (x._1, x._2 + "", "mix_period_count"))

    end
  }

  //极短周期百分比(只取极短特征个数大于1的企业)
  def mix_period_rate(ods_policy_detail: DataFrame, ods_policy_risk_period: DataFrame): RDD[(String, String, String)] = {

    val tepOne = ods_policy_detail.select("ent_id", "policy_code")
    val tepTwo = ods_policy_risk_period.select("policy_no", "risk_period")
    val tepThree = tepOne.join(tepTwo, ods_policy_detail("policy_code") === ods_policy_risk_period("policy_no"))
      .filter("length(ent_id)>0")
    //    |              ent_id|         policy_code|           policy_no|risk_period|
    //    |0a789d56b7444d519...|  900000047702719243|  900000047702719243|         57|
    val total: RDD[(String, Double)] = tepThree
      .map(x => {
        val risk_period = x.get(3).toString.toDouble
        val ent_id = x.getString(0).toString
        (ent_id, risk_period)
      })
    //求出周期小于3的次数,并求出有多少个
    val threeNumber = total
      .filter(_._2 < 3.0)
      .map(x => (x._1, 1.0))
      .reduceByKey(_ + _)
    //总共有多少个
    val sum_Number = total.map(x => (x._1, 1))
      .reduceByKey(_ + _)
      .map(x => (x._1, x._2))
    //ent_id | 百分比 |字段
    val end: RDD[(String, String, String)] = threeNumber
      .join(sum_Number)
      //ent_id | 小于3的次数| 总次数
      .map(x => (x._1, x._2._1, x._2._2))
      //将次数大于1的过滤出来
      .filter(x => if (x._2 > 1) true else false)
      .map(x => {
        //概率
        val end = x._2 / x._3
        (x._1, end + "", "mix_period_rate")
      })
    end
  }

  //重大案件率
  def largecase_rate(ods_policy_detail: DataFrame, employer_liability_claims: DataFrame): RDD[(String, String, String)] = {
    val numberFormat = NumberFormat.getInstance
    numberFormat.setMaximumFractionDigits(6)

    val tepOne = ods_policy_detail.select("ent_id", "policy_code")
    val tepTwo = employer_liability_claims.select("policy_no", "case_type")
    val tepThree = tepOne.join(tepTwo, ods_policy_detail("policy_code") === employer_liability_claims("policy_no")).filter("length(ent_id) > 0")
    //该企业发生死亡的人数
    //    |              ent_id|         policy_code|           policy_no|case_type|
    //    |0a789d56b7444d519...|  900000047702719243|  900000047702719243|       创伤|
    val dath_number = tepThree
      .map(x => (x.getString(0), x.getString(3)))
      .filter(x => if (x._2 == "死亡") true else false)
      .map(x => (x._1, 1))
      .reduceByKey(_ + _)
      .map(x => (x._1, x._2))
    //该企业发生残疾的人数
    val can_number = tepThree
      .map(x => (x.getString(0), x.getString(3)))
      .filter(x => if (x._2 == "残疾") true else false)
      .map(x => (x._1, 1))
      .reduceByKey(_ + _)
      .map(x => (x._1, x._2))
    //该企业的总次数
    val total_number = tepThree
      .map(x => (x.getString(0), x.getString(3)))
      .map(x => (x._1, 1))
      .reduceByKey(_ + _)
      .map(x => x)

    val tepFour = dath_number
      .join(can_number)
      //企业ID，该企业死亡人数，该企业残疾人数
      .map(x => (x._1, (x._2._1, x._2._2)))
      .filter(x => if (x._2._1 > 0 || x._2._2 > 0) true else false)

    val end: RDD[(String, String, String)] = tepFour
      .join(total_number)
      .map(x => {
        //企业ID，该企业死亡人数，该企业残疾人数,总人数
        val ent_id = x._1
        val deathNum = x._2._1._1.toDouble
        val disabilityNum = x._2._1._2.toDouble
        val totalNum = x._2._2
        val gailv = (deathNum + disabilityNum) / totalNum.toDouble
        //      (ent_id, numberFormat.format(gailv), "largecase_rate")
        (ent_id, gailv.toString, "largecase_rate")
      })

    end
  }

  //工种风险等级入hbase
  def ent_work_risk_r_To_Hbase(ent_work_risk_r: RDD[(String, String, String)], columnFamily: String,
                               conf_fs: Configuration, tableName: String, conf: Configuration): Unit = {

    val zero_f = ent_work_risk_r.filter(_._3 == "0").map(x => (x._1, x._2, s"worktype_${x._3}_count")).distinct
    toHbase(zero_f, columnFamily, "ent_work_risk_zero", conf_fs, tableName, conf)

    val one_f = ent_work_risk_r.filter(_._3 == "1").map(x => (x._1, x._2, s"worktype_${x._3}_count")).distinct
    toHbase(one_f, columnFamily, "ent_work_risk_one", conf_fs, tableName, conf)

    val two_f = ent_work_risk_r.filter(_._3 == "2").map(x => (x._1, x._2, s"worktype_${x._3}_count")).distinct
    toHbase(two_f, columnFamily, "ent_work_risk_two", conf_fs, tableName, conf)

    val three_f = ent_work_risk_r.filter(_._3 == "3").map(x => (x._1, x._2, s"worktype_${x._3}_count")).distinct
    toHbase(three_f, columnFamily, "ent_work_risk_three", conf_fs, tableName, conf)

    val four_f = ent_work_risk_r.filter(_._3 == "4").map(x => (x._1, x._2, s"worktype_${x._3}_count")).distinct
    toHbase(four_f, columnFamily, "ent_work_risk_four", conf_fs, tableName, conf)

    val five_f = ent_work_risk_r.filter(_._3 == "5").map(x => (x._1, x._2, s"worktype_${x._3}_count")).distinct
    toHbase(five_f, columnFamily, "ent_work_risk_five", conf_fs, tableName, conf)

    val six_f = ent_work_risk_r.filter(_._3 == "6").map(x => (x._1, x._2, s"worktype_${x._3}_count")).distinct
    toHbase(six_f, columnFamily, "ent_work_risk_six", conf_fs, tableName, conf)

    val seven_f = ent_work_risk_r.filter(_._3 == "7").map(x => (x._1, x._2, s"worktype_${x._3}_count")).distinct
    toHbase(seven_f, columnFamily, "ent_work_risk_seven", conf_fs, tableName, conf)
  }

}
