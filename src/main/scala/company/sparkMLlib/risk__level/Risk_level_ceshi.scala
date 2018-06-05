package company.sparkMLlib.risk__level

import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MK on 2018/5/14.
  */
object Risk_level_ceshi extends risl_until {
  //找到最优的分类
  def Optimal_classification(train_date: RDD[linalg.Vector], test_date: RDD[linalg.Vector]): Unit
  = {
    /**
      * K-均值在交叉验证的情况，WCSS随着K的增大持续减小，但是达到某个值后，下降的速率突然会变得很平缓。这时的K通常为最优的K值（这称为拐点）。k最佳为10左右，
      * 尽管较大的K值从数学的角度可以得到更优的解，但是类簇太多就会变得难以理解和解释
      **/
    val userCosts = Seq(1, 3, 5, 7, 9).map {
      param =>
        (param, KMeans.train(train_date, param, 50).computeCost(test_date))
    }
    println("User clustering cross-validation:")
    userCosts.foreach {
      case (k, cost) =>
        val res = f"WCSS for K=$k id $cost%2.2f"
        System.out.println(res)
    }
  }


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


    val scaler_M = new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures")
    val scalerModel = scaler_M.fit(result)
    val scaledData = scalerModel.transform(result)
    //      .map(x => x.getAs[org.apache.spark.mllib.linalg.Vector]("scaledFeatures"))


    //加载报案时效的模型："hdfs://namenode1.cdh:8020/model/risk_level"
    val risk_level_model = KMeansModel.load(sc, "hdfs://namenode1.cdh:8020/model/risk_level")

    scaledData.map(x => {
     val features =  x.getAs("features").toString
     val scaledFeatures =  x.getAs[org.apache.spark.mllib.linalg.Vector]("scaledFeatures")

      val value = risk_level_model.predict(scaledFeatures)
      (value,features)
    }).take(20).foreach(println(_))

    //        val all = scaledData.randomSplit(Array(0.8, 0.2), 11L)
    //        val train_date = all(0).cache
    //        val test_date = all(1)
    //        Optimal_classification(train_date: RDD[linalg.Vector], test_date: RDD[linalg.Vector])


    //    val numCluster = 3
    //    //        最大的分类数(设置多少个中心点，也是KMeans中的K)
    //    val numTerations = 50 //最大的迭代次数
    //    val clusters: KMeansModel = KMeans.train(scaledData, numCluster, numTerations) //训练模型
    //    clusters.save(sc, "hdfs://namenode1.cdh:8020/model/risk_level")

    //    scalerModel.transform(result).map(x => {
    //      (clusters.predict(x.getAs[linalg.Vector]("scaledFeatures")), x.getAs("features").toString)
    //      x.getAs("features").toString
    //    }).foreach(println(_))
    //      .repartition(1).saveAsTextFile("C:\\Users\\a2589\\Desktop\\需求one\\a3")

  }
}
