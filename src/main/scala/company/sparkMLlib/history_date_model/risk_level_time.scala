package company.sparkMLlib.history_date_model

import java.util.Properties

import company.sparkMLlib.risk__level.risl_until
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MK on 2018/5/24.
  */
object risk_level_time  extends risl_until{

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf_spark = new SparkConf().setAppName("wuYu")
    conf_spark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf_spark.registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
    conf_spark.set("spark.sql.broadcastTimeout", "36000").setMaster("local[2]")

    val sc = new SparkContext(conf_spark)
    val sqlContext: HiveContext = new HiveContext(sc)


    val employer_liability_claims = sqlContext.sql("select * from odsdb_prd.employer_liability_claims").filter("length(risk_date)>3 and length(report_date)>3").cache
    val ods_policy_insured_detail = sqlContext.sql("select * from odsdb_prd.ods_policy_insured_detail").filter("length(insured_start_date)>3").select("insured_cert_no", "insured_start_date")

    import sqlContext.implicits._
    //得到我hbase中的报案时效数据
    val tep_One = employer_liability_claims.map(x => {
      //身份证号
      val cert_no = if (x.getAs[String]("cert_no").length == 0) "0" else x.getAs[String]("cert_no")

      val final_payment = if (x.getAs("final_payment").toString.length == 0) "0" else x.getAs("final_payment").toString
      //赔付金额
      val pre_com_before = if (x.getAs[String]("pre_com").length == 0) "0" else x.getAs[String]("pre_com")
      val pre_com = if (final_payment == "0") pre_com_before else final_payment




      //报案日期
      val report_date = if (x.getAs[String]("report_date").length == 0) "0" else x.getAs[String]("report_date")
      //出险日期
      val risk_date = if (x.getAs[String]("risk_date").length == 0) "0" else x.getAs[String]("risk_date")
      //出险日期要比报案如期要早 ，通过计算可得2者天数，也是报案时效天数
      val risk_num_date = getBeg_End_one_two(risk_date, report_date).length.toString

      (cert_no, pre_com, risk_date, risk_num_date, report_date)
    }).toDF("insured_cert_no", "pre_com", "risk_date", "risk_num_date", "report_date")
    val result = ods_policy_insured_detail.join(tep_One, "insured_cert_no").map(x => {
      val insured_cert_no = x.getAs[String]("insured_cert_no")
      //保险起期
      val insured_start_date = x.getAs[String]("insured_start_date").split(" ")(0).replaceAll("-", "/")
      //      val insured_start_date = x.getAs[String]("insured_start_date")
      //赔付金额
      val pre_com = x.getAs[String]("pre_com")
      val risk_date = x.getAs[String]("risk_date")
      //报案时效
      val risk_num_date = x.getAs[String]("risk_num_date")

      val report_date = x.getAs[String]("report_date")

      val insured_start_date_L = currentTimeL(s"$insured_start_date 00:00:00")
      val risk_date_L = currentTimeL(s"$risk_date 00:00:00")

      (
        insured_cert_no,
        (
          insured_start_date_L,
          risk_date_L,
          risk_date_L - insured_start_date_L,
          insured_start_date,
          risk_date,
          pre_com,
          report_date
        )
      )
      //出险日期>保险起期的找出来
    }).filter(x => {
      if (x._2._2 > x._2._1) true else false
    })
      //过滤出来以后，再根据身份证分组，找到我2日期相减最小天数的一行数据
      .reduceByKey((x1, x2) => {
      if (x1._3 <= x2._3) x1 else x2
    }).map(x => {
      val insured_start_date = x._2._4
      val risk_date = x._2._5
      //报案日期
      val report_date = x._2._7

      //出险日期要比报案如期要早 ，通过计算可得2者天数，也是报案时效天数
      val baoan_time = getBeg_End_one_two(risk_date, report_date).length.toString


      //出险天数,保险起期早，出险日期晚.
      val chuxian_num_days = getBeg_End_one_two(insured_start_date, risk_date).length.toString

      //预估赔付额度
      val pre_com = x._2._6

      (x._1, Vectors.dense(Array(baoan_time, chuxian_num_days, pre_com).map(_.toDouble)))
    }).toDF("label", "features")

    //对数据进行归一化处理
    val scaler_M = new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures")
    val scalerModel = scaler_M.fit(result)
    val scaledData = scalerModel.transform(result)

    //加载报案时效模型，并进行预测
    val risk_level_model = KMeansModel.load(sc, "hdfs://namenode1.cdh:8020/model/risk_level")
    val tep_five = scaledData.map(x => {
      val scaledFeatures = x.getAs[org.apache.spark.mllib.linalg.Vector]("scaledFeatures")

      val cet_no = x.getAs[String]("label")
      val features = x.getAs("features").toString
      val value = risk_level_model.predict(scaledFeatures).toString
      (cet_no, features, value)
    }).toDF("cert_no", "features", "value")


    //加载老版的数据，目的是与新版的进行匹配观察
    val url_one = "jdbc:mysql://172.16.11.105:3306/dwdb?user=root&password=bzn@cdh123!"
    val prop = new Properties
    val vc_verifyclaim = sqlContext.read.jdbc(url_one, "vc_verifyclaim", prop).select("insured_certno", "riskInfo_level").where("riskInfo_level in (1,2,3)")
      .map(x => {
        val cert_no = x.getAs("insured_certno").toString
        val riskInfo_level = x.getAs("riskInfo_level").toString
        (cert_no, (cert_no, riskInfo_level))
      }).filter(_._2._2 != null).reduceByKey((x1, x2) => {
      if (x1._1 == x2._1) x1 else x2
    }).map(x => (x._1, x._2._2)).toDF("cert_no", "old_value")

    val end_tep = tep_five.join(vc_verifyclaim, "cert_no")

    end_tep.repartition(1).write.format("com.databricks.spark.csv")
      .option("header", "true") //在csv第一行有属性"true"，没有就是"false"
      .option("delimiter", ",") //默认以","分割
      .save("C:\\Users\\a2589\\Desktop\\报案时效")

  //分为了3类0,1,2，越大的等级越高
  }
}
