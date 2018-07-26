package company.hbase_label.enterprise

import java.text.NumberFormat
import java.util.regex.Pattern

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by MK on 2018/7/23.
  */
object cece {
  def finalpay_total(employer_liability_claims: DataFrame): RDD[(String, String, String)] = {
    val end = employer_liability_claims.filter("length(cert_no)> 2").select("cert_no", "final_payment").map(x => {
      val result = if (x.get(1) == "" || x.get(1)==null) 0 else x.get(1).toString.toDouble
      (x.getString(0), result)
    }).reduceByKey(_ + _).map(x => {
      (x._1, x._2.toInt + "", "finalpay_total")
    })
    //      .take(10).foreach(println(_))
    end
  }

  def main(args: Array[String]): Unit = {
    //
    val conf_s = new SparkConf().setAppName("wuYu")

    conf_s.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //使用spark的序列化
    conf_s.set("spark.sql.broadcastTimeout", "36000") //等待时长
      .setMaster("local[4]")
    val sc = new SparkContext(conf_s)
    val sqlContext: HiveContext = new HiveContext(sc)

    val employer_liability_claims: DataFrame = sqlContext.sql("select * from odsdb_prd.employer_liability_claims").cache


    finalpay_total(employer_liability_claims: DataFrame).foreach(println(_))

  }
}
