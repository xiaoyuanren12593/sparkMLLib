package personal

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MK on 2018/5/2.
  */
object read_dataFrom_hive_create_model_MinMaxScaler {
  //找到最优的分类
  def Optimal_classification(train_date: RDD[linalg.Vector], test_date: RDD[linalg.Vector]): Unit = {
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

  //用来记录簇的索引,并显示中心簇的坐标
  def Central_cluster(clusters: KMeansModel): Unit = {
    var clusterIndex: Int = 0
    println("cluster Number:  ", clusters.clusterCenters.length) //打印分类数
    println("Cluster Centers Information Overview:")
    clusters.clusterCenters.foreach(
      x => {

        println("Center Point of Cluster " + clusterIndex + ":")
        println(x)
        clusterIndex += 1
      })
  }

  //result show:预测数据
  def Result_show(test_date: RDD[linalg.Vector], clusters: KMeansModel, sc: SparkContext): Unit = {
    test_date.take(200).map(testDataLine => {
      val predictedClusterIndex: Int = clusters.predict(testDataLine)
      ("The data ", testDataLine.toString, "belongs to cluster", predictedClusterIndex)
    }).foreach(println(_))

    val st = sc.textFile("F:\\tmp\\company\\m.txt").map(x => x.split(",").map(_.toDouble)).map(x => Vectors.dense(x))
    val scalers = new StandardScaler(withMean = true, withStd = true).fit(st)
    val scaled_datas: RDD[linalg.Vector] = st.map(point => scalers.transform(point))

    val sse = scaled_datas.map(x =>
      //      clusters.predict(x)
      x
    ).foreach(println(_))
    scaled_datas.map(x => {
      clusters.predict(x)
    }).foreach(println(_))
  }


  //创建模型
  def create_model(sc: SparkContext): KMeansModel
  = {
    //城市编码
    val sqlContext = new HiveContext(sc)
    val res = sqlContext.sql("select * from personal_model_data")
    import sqlContext.implicits._
    val vectors: DataFrame = res.map(x => {
      //去掉表中的最后2列
      //      val all_arr = x.mkString("\t").split("\t").toBuffer
      //      all_arr.remove(all_arr.length-1)
      //      all_arr.remove(all_arr.length-2)

      //得到特征维度
      val money = x.getAs("money").toString
      val early_res = x.getAs("early_res").toString
      val user_craft_level = x.getAs("user_craft_level").toString
      val age = x.getAs("age").toString
      val city_level = x.getAs("city_level").toString
      val sex = x.getAs("sex").toString
      val Insure_to_month = x.getAs("insure_to_month").toString
      val res = Array(money, early_res, user_craft_level, age, city_level, sex, Insure_to_month).map(_.toDouble)


      //      val res = all_arr.map(_.toDouble).toArray
      //得到表中的身份证
      val label = x.getAs("cert_no").toString
      ("", Vectors.dense(res))
    }).toDF("", "features").cache

    val scaler_M = new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures")
    val scalerModel = scaler_M.fit(vectors)
    val scaledData = scalerModel.transform(vectors)

    val numCluster = 7
    //最大的分类数(设置多少个中心点，也是KMeans中的K)
    val numTerations = 50 //最大的迭代次数
    val clusters: KMeansModel = KMeans.train(scaledData.map(x => x.getAs[linalg.Vector]("scaledFeatures")), numCluster, numTerations) //训练模型
    clusters
  }


  //得到vectors
  def getVectors(sc: SparkContext): DataFrame = {
    //城市编码
    val sqlContext = new HiveContext(sc)
    val res = sqlContext.sql("select * from personal_model_data")
    import sqlContext.implicits._
    val vectors: DataFrame = res.map(x => {
      //去掉表中的最后2列
      //      val all_arr = x.mkString("\t").split("\t").toBuffer
      //      all_arr.remove(all_arr.length-1)
      //      all_arr.remove(all_arr.length-2)

      //得到特征维度
      val money = x.getAs("money").toString
      val early_res = x.getAs("early_res").toString
      val user_craft_level = x.getAs("user_craft_level").toString
      val age = x.getAs("age").toString
      val city_level = x.getAs("city_level").toString
      val sex = x.getAs("sex").toString
      val Insure_to_month = x.getAs("insure_to_month").toString
      val res = Array(money, early_res, user_craft_level, age, city_level, sex, Insure_to_month).map(_.toDouble)


      //      val res = all_arr.map(_.toDouble).toArray
      //得到表中的身份证
      val label = x.getAs("cert_no").toString
      ("", Vectors.dense(res))
    }).toDF("", "features").cache
    val scaler_M = new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures")
    val scalerModel = scaler_M.fit(vectors)
    val scaledData = scalerModel.transform(vectors)
    scaledData
  }

  /**
    * 不需要标准化的。聚类主要是看观察值的聚类。如果想用标准化数据来做，
    * 可以在SPSS聚类分析中选择0-1标准化的值来处理，而不需要事先在原始数据中标准化。
    * 此外，如果数据中存在极端值，标准化之后的数据依然存在极端值，因此，极端值的处理不是通过标准化而是通过删除处理的。
    * 另外，聚类分析对原始数据的原始值非常重视，
    * 即便是极端值，它们本身也是“特殊一类”，所以，对极端值的处理要慎重。
    **/
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf_spark = new SparkConf().setAppName("wuYu")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf_spark)

    val model: KMeansModel = create_model(sc: SparkContext)

    model.save(sc, "hdfs://namenode1.cdh:8020/model/person_risk")


    //    val scaler_tep = new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures")
    //    val scaler_tep_model = scaler_tep.fit(testData)
    //    val scaledData_model = scaler_tep_model.transform(testData)
    //
    //    scaledData_model.map(x => {
    //      val yc: linalg.Vector = x.getAs[linalg.Vector]("scaledFeatures")
    //      //预测值 | 归一化后的值 | 原始数据
    //      //      (clusters.predict(yc)+1, yc, x.getAs[linalg.Vector]("features").toArray.mkString("\t"))
    //      s"${clusters.predict(yc) + 1}\t${x.getAs[linalg.Vector]("features").toArray.mkString("\t")}"
    //    }).repartition(1).saveAsTextFile("F:\\tmp\\company\\ccA")


    //找到最优的分类
    //得到vectors
    //    val vectors = getVectors(sc: SparkContext)
    //    val testData = vectors.randomSplit(Array(0.8, 0.2), 11L)
    //    val train_date = testData(0).map(x=>{x.getAs[linalg.Vector]("scaledFeatures")}).cache
    //    val test_date = testData(1).map(x=>{x.getAs[linalg.Vector]("scaledFeatures")})
    //    Optimal_classification(train_date: RDD[linalg.Vector], test_date: RDD[linalg.Vector])

    sc.stop()
  }
}
