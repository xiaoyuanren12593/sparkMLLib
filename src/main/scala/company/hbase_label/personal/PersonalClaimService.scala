package company.hbase_label.personal

import java.text.NumberFormat
import java.util.regex.Pattern

import company.hbase_label.until
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object PersonalClaimService extends until {
  //个人报案件数
  def report_count(employer_liability_claims: DataFrame): RDD[(String, String, String)] = {
    val end: RDD[(String, String, String)] = employer_liability_claims.filter("length(cert_no)>2").select("cert_no").map(x => {
      (x.getString(0), 1)
    }).reduceByKey(_ + _).map(x => {
      (x._1, x._2 + "", "report_count")
    })
    end
  }

  //理赔件数
  def claim_count(employer_liability_claims: DataFrame): RDD[(String, String, String)] = {
    //paper_finish_date:资料补全日期
    val end = employer_liability_claims.filter("length(cert_no)>2 and length(paper_finish_date)>0").select("cert_no").map(x => {
      (x.getString(0), 1)
    }).reduceByKey(_ + _).map(x => {
      (x._1, x._2 + "", "claim_count")
    })
    end
    //    .take(10).foreach(println(_))

  }

  //死亡案件
  def death_count(employer_liability_claims: DataFrame): RDD[(String, String, String)] = {
    val end = employer_liability_claims.filter("length(cert_no)>2 and case_type='死亡'").select("cert_no").map(x => {
      (x.getString(0), 1)
    }).reduceByKey(_ + _).map(x => {
      (x._1, x._2 + "", "death_count")
    })
    end
  }

  //伤残案件
  def disability_count(employer_liability_claims: DataFrame): RDD[(String, String, String)] = {
    val end: RDD[(String, String, String)] = employer_liability_claims.filter("length(cert_no)> 2").where("disable_level not in ('无','死亡')").select("cert_no").map(x => {
      (x.getString(0), 1)
    }).reduceByKey(_ + _).map(x => {
      (x._1, x._2 + "", "disability_count")
    })
    end
  }

  //工作期间案件数
  def case_work_count(employer_liability_claims: DataFrame): RDD[(String, String, String)] = {
    val end: RDD[(String, String, String)] = employer_liability_claims.filter("length(cert_no)> 2").where("scene='工作期间'").select("cert_no").map(x => {
      (x.getString(0), 1)
    }).reduceByKey(_ + _).map(x => {
      (x._1, x._2 + "", "case_work_count")
    })
    end
  }

  //非工作期间案件数
  def case_notwork_count(employer_liability_claims: DataFrame): RDD[(String, String, String)] = {
    val end = employer_liability_claims.filter("length(cert_no)> 2").where("scene in ('非工作期间', '上下班')").select("cert_no").map(x => {
      (x.getString(0), 1)
    }).reduceByKey(_ + _).map(x => {
      (x._1, x._2 + "", "case_notwork_count")
    })
    end
  }

  //预估总赔付金额
  //pre_com:预估赔付金额
  def prepay_total(employer_liability_claims: DataFrame): RDD[(String, String, String)] = {
    val end = employer_liability_claims.filter("length(cert_no)> 14").filter("pre_com is not null").select("cert_no", "pre_com")
      .map(x => x)
      .filter(x => {
        val pre_com = x.get(1).toString
        val p = Pattern.compile("[\u4e00-\u9fa5]")
        val m = p.matcher(pre_com)
        if (!m.find) true else false
      })
      .map(x => {
        (x.getString(0).trim(), x.get(1).toString)
      })
      .filter(_._2 != ".").filter(_._2 != "#N/A")
      .map(x => (x._1, x._2.toDouble))
      .reduceByKey(_ + _).map(x => {
      (x._1, x._2.toInt + "", "prepay_total")
    })
    end
  }

  //死亡预估赔额
  def prepay_death(employer_liability_claims: DataFrame): RDD[(String, String, String)] = {
    val end = employer_liability_claims.filter("length(cert_no)> 2").where("case_type='死亡'").select("cert_no", "pre_com")
      .map(x => x)
      .filter(x => {
        val pre_com = x.get(1).toString
        val p = Pattern.compile("[\u4e00-\u9fa5]")
        val m = p.matcher(pre_com)
        if (!m.find) true else false
      })
      .map(x => {
        (x.getString(0), x.get(1).toString)
      })
     .filter(_._2 != ".").filter(_._2 != "#N/A")
      .map(x => (x._1, x._2.toDouble))

      .reduceByKey(_ + _).map(x => {
      (x._1, x._2.toInt + "", "prepay_death")
    })
    end
  }

  //伤残预估赔额
  def prepay_disability(employer_liability_claims: DataFrame): RDD[(String, String, String)] = {
    val end = employer_liability_claims.filter("length(cert_no)> 2").where("disable_level not in ('无','死亡')").select("cert_no", "pre_com")
      .map(x => x)
      .filter(x => {
        val pre_com = x.get(1).toString
        val p = Pattern.compile("[\u4e00-\u9fa5]")
        val m = p.matcher(pre_com)
        if (!m.find) true else false
      })

      .map(x => {
        (x.getString(0), x.get(1).toString)
      })
      .filter(_._2 != ".").filter(_._2 != "#N/A")
      .map(x => (x._1, x._2.toDouble))

      .reduceByKey(_ + _).map(x => {
      (x._1, x._2.toInt + "", "prepay_disability")
    })
    end
  }

  //工作期间预估赔额
  def prepay_work(employer_liability_claims: DataFrame): RDD[(String, String, String)] = {
    val end = employer_liability_claims.filter("length(cert_no)> 2").where("scene='工作期间'").select("cert_no", "pre_com")

      .map(x => x)
      .filter(x => {
        val pre_com = x.get(1).toString
        val p = Pattern.compile("[\u4e00-\u9fa5]")
        val m = p.matcher(pre_com)
        if (!m.find) true else false
      })
      .map(x => {
        (x.getString(0), x.get(1).toString)
      })
      .filter(_._2 != ".").filter(_._2 != "#N/A")
      .map(x => (x._1, x._2.toDouble))

      .reduceByKey(_ + _).map(x => {
      (x._1, x._2.toInt + "", "prepay_work")
    })
    end
  }

  //非工作期间预估赔额
  def prepay_notwork(employer_liability_claims: DataFrame): RDD[(String, String, String)] = {
    val end = employer_liability_claims.filter("length(cert_no)> 2").where("scene in ('非工作期间', '上下班')").select("cert_no", "pre_com")
      .map(x => x)
      .filter(x => {
        val pre_com = x.get(1).toString
        val p = Pattern.compile("[\u4e00-\u9fa5]")
        val m = p.matcher(pre_com)
        if (!m.find) true else false
      })

      .map(x => {
        (x.getString(0), x.get(1).toString)
      })
      .filter(_._2 != ".").filter(_._2 != "#N/A")
      .map(x => (x._1, x._2.toDouble))
      .reduceByKey(_ + _).map(x => {
      (x._1, x._2.toInt + "", "prepay_notwork")
    })
    end
  }

  //实际已赔付金额
  def finalpay_total(employer_liability_claims: DataFrame): RDD[(String, String, String)] = {
    val end = employer_liability_claims.filter("length(cert_no)> 2").select("cert_no", "final_payment").map(x => {
      val result = if (x.get(1) == "" || x.get(1) == null) 0 else x.get(1).toString.toDouble
      (x.getString(0), result)
    }).reduceByKey(_ + _).map(x => {
      (x._1, x._2.toInt + "", "finalpay_total")
    })
    //      .take(10).foreach(println(_))
    end
  }

  //预估平均案件金额
  def avg_prepay(employer_liability_claims: DataFrame): RDD[(String, String, String)] = {

    val end = employer_liability_claims.filter("length(cert_no)> 2").select("cert_no", "pre_com")
      .map(x => x)
      .filter(x => {
        val pre_com = x.get(1).toString
        val p = Pattern.compile("[\u4e00-\u9fa5]")
        val m = p.matcher(pre_com)
        if (!m.find) true else false
      })
    val numberFormat = NumberFormat.getInstance
    numberFormat.setMaximumFractionDigits(2)

    val yG = end.map(x => {
      val result = if (x.get(1) == "" || x.get(1) == null) "0.0" else x.get(1).toString
      (x.getString(0), result)
    })
      .filter(_._2 != ".").filter(_._2 != "#N/A")
      .map(x => (x._1, x._2.toDouble))
      .reduceByKey(_ + _).map(x => {
      (x._1, x._2.toDouble)
    })

    val num_Y = end.map(x => {
      (x.getString(0), 1)
    }).reduceByKey(_ + _).map(x => {
      (x._1, x._2.toDouble)
    })
    val ends: RDD[(String, String, String)] = yG.join(num_Y).map(x => {
      val yG = x._2._1
      val num_total = x._2._2
      val res = yG / num_total
      (x._1, numberFormat.format(res), "avg_prepay")
    })
    ends

  }

  //实际平均案件金额
  def avg_finalpay(employer_liability_claims: DataFrame): RDD[(String, String, String)] = {
    val end = employer_liability_claims.filter("length(cert_no)> 2").select("cert_no", "final_payment")
    val numberFormat = NumberFormat.getInstance
    numberFormat.setMaximumFractionDigits(2)

    val yG = end.map(x => {
      val result = if (x.get(1) == "" || x.get(1) == null) "0.0" else x.get(1).toString
      (x.getString(0), result)
    })
      .filter(_._2 != ".")
      .map(x => (x._1, x._2.toDouble))

      .reduceByKey(_ + _).map(x => {
      (x._1, x._2.toDouble)
    })

    val num_Y = end.map(x => {
      (x.getString(0), 1)
    }).reduceByKey(_ + _).map(x => {
      (x._1, x._2.toDouble)
    })
    val ends: RDD[(String, String, String)] = yG.join(num_Y).map(x => {
      val yG = x._2._1
      val num_total = x._2._2
      val res = yG / num_total
      (x._1, numberFormat.format(res), "avg_finalpay")
    })
    ends
  }

  //超时赔付案件数
  def case_overtime_count(employer_liability_claims: DataFrame): RDD[(String, String, String)] = {
    val end = employer_liability_claims.filter("overtime='是'").where("length(cert_no)>2").select("cert_no").map(x => {
      (x.getString(0), 1)
    }).reduceByKey(_ + _).map(x => {
      (x._1, x._2 + "", "case_overtime_count")
    })
    end
  }

  //平均赔付时效
  def avg_effecttime(employer_liability_claims: DataFrame): RDD[(String, String, String)] = {
    val numberFormat = NumberFormat.getInstance
    numberFormat.setMaximumFractionDigits(2)

    val end = employer_liability_claims.filter("length(cert_no) >2 ").select("cert_no", "time_effect").map(x => {
      val res = if (x.get(1) == "" || x.get(1) == null) 0.0 else x.get(1).toString.toDouble
      (x.getString(0), (res, 1))
    }).reduceByKey((x1, x2) => {
      val sum = x1._1 + x2._1
      val count = x1._2 + x2._2
      (sum, count)
    }).map(x => {
      val avg = x._2._1 / x._2._2
      val cert_no = x._1
      (cert_no, numberFormat.format(avg), "avg_effecttime")
    })
    end
  }

  def main(args: Array[String]): Unit = {
    val conf_s = new SparkConf().setAppName("wuYu")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
      .set("spark.sql.broadcastTimeout", "36000")
      .set("spark.network.timeout", "36000")
    //      .setMaster("local[2]")
    val sc = new SparkContext(conf_s)
    val sqlContext: HiveContext = new HiveContext(sc)

    val conf = HbaseConf("labels:label_user_personal_vT")._1
    val conf_fs = HbaseConf("labels:label_user_personal_vT")._2
    val tableName = "labels:label_user_personal_vT"
    val columnFamily1 = "baseinfo"

    //employer_liability_claims	雇主保理赔记录表
    val employer_liability_claims = sqlContext.sql("select * from odsdb_prd.employer_liability_claims")


    //个人报案件数
    val report_count_s = report_count(employer_liability_claims)
    toHbase(report_count_s, columnFamily1, "report_count", conf_fs, tableName, conf)

    //理赔件数
    val claim_count_rs = claim_count(employer_liability_claims)
    toHbase(claim_count_rs, columnFamily1, "claim_count", conf_fs, tableName, conf)

    //死亡案件
    val death_count_s = death_count(employer_liability_claims)
    toHbase(death_count_s, columnFamily1, "death_count", conf_fs, tableName, conf)

    //伤残案件
    val disability_count_s = disability_count(employer_liability_claims)
    toHbase(disability_count_s, columnFamily1, "disability_count", conf_fs, tableName, conf)

    //工作期间案件数
    val case_work_count_s = case_work_count(employer_liability_claims)
    toHbase(case_work_count_s, columnFamily1, "case_work_count", conf_fs, tableName, conf)

    //非工作期间案件数
    val case_notwork_count_s = case_notwork_count(employer_liability_claims)
    toHbase(case_notwork_count_s, columnFamily1, "case_notwork_count", conf_fs, tableName, conf)

    //预估总赔付金额
    //pre_com:预估赔付金额
    val prepay_total_s = prepay_total(employer_liability_claims)
    toHbase(prepay_total_s, columnFamily1, "prepay_total", conf_fs, tableName, conf)

    //死亡预估赔额
    val prepay_death_s = prepay_death(employer_liability_claims)
    toHbase(prepay_death_s, columnFamily1, "prepay_death", conf_fs, tableName, conf)

    //伤残预估赔额
    val prepay_disability_s = prepay_disability(employer_liability_claims)
    toHbase(prepay_disability_s, columnFamily1, "prepay_disability", conf_fs, tableName, conf)

    //工作期间预估赔额
    val prepay_work_s = prepay_work(employer_liability_claims)
    toHbase(prepay_work_s, columnFamily1, "prepay_work", conf_fs, tableName, conf)

    //非工作期间预估赔额
    val prepay_notwork_s = prepay_notwork(employer_liability_claims)
    toHbase(prepay_notwork_s, columnFamily1, "prepay_notwork", conf_fs, tableName, conf)

    //实际已赔付金额
    val finalpay_total_s = finalpay_total(employer_liability_claims)
    toHbase(finalpay_total_s, columnFamily1, "finalpay_total", conf_fs, tableName, conf)

    //预估平均案件金额
    val avg_prepay_s = avg_prepay(employer_liability_claims)
    toHbase(avg_prepay_s, columnFamily1, "avg_prepay", conf_fs, tableName, conf)

    //实际平均案件金额
    val avg_finalpay_s = avg_finalpay(employer_liability_claims)
    toHbase(avg_finalpay_s, columnFamily1, "avg_finalpay", conf_fs, tableName, conf)

    //超时赔付案件数
    val case_overtime_count_s = case_overtime_count(employer_liability_claims)
    toHbase(case_overtime_count_s, columnFamily1, "case_overtime_count", conf_fs, tableName, conf)

    //平均赔付时效
    //time_effect:赔付时效
    val avg_effecttime_s = avg_effecttime(employer_liability_claims)
    toHbase(avg_effecttime_s, columnFamily1, "avg_effecttime", conf_fs, tableName, conf)

  }

}
