package personal

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MK on 2018/5/2.
  */
object read_dataFrom_hive_create_model_StandardScaler {
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

  //用来记录簇的索引,并显示中心簇的坐标
  def Central_cluster(clusters: KMeansModel): Unit
  = {
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
  def Result_show(test_date: RDD[linalg.Vector], clusters: KMeansModel, sc: SparkContext): Unit
  = {
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

  /**
    * 如果对输出结果范围有要求，用归一化
    * 如果数据较为稳定，不存在极端的最大值和最小值，用归一化
    * 如果数据存在异常值和较多噪音，用标准化，可以间接通过中心化，避免异常值和极端的影响
    * 一般推荐使用标准化
    **/
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf_spark = new SparkConf().setAppName("wuYu")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf_spark)
    //城市编码
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    val res = sqlContext.sql("select * from personal_model_data")
    //    |finalpay_total|early_res|user_craft_level|claim_count|prepay_total|age|city_level|           cert_no|
    //    |             0|        0|               0|          0|           0| 29|         1|110000198908265175|
    val res_k_means = res.map(x => {
      //去掉表中的最后一列
      val all_arr = x.mkString("\t").split("\t").toBuffer
      all_arr.remove(7)
      val res = all_arr.map(_.toDouble).toArray
      //得到表中的最后一列
      val label = x.getAs("cert_no").toString
      Vectors.dense(res)
    }).cache()


    /*特征标准化优化*/
    val vectors = res_k_means.map(x => x)

    //withMean：默认为假。此种方法将产出一个稠密输出，所以不适用于稀疏输入。
    //withStd：默认值为真，使用统一标准差方式。

    /**
      * withMean 如果值为true，那么将会对列中每个元素减去均值（否则不会减）
      * withStd 如果值为true，那么将会对列中每个元素除以标准差（否则不会除，这个值一般为 true，否则没有标准化没有意义） 所以上面两个参数都为 false 是没有意义的，模型什么都不会干，返回原来的值，这些将会在下面的代码中得到验证。
      **/
    //标准化,可以对RDD进行标准化，也可以单独对向量进行标准化
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(vectors).transform(vectors)
    val date = scaler.randomSplit(Array(0.8, 0.2), 11L)
    val train_date = date(0)
    val test_date = date(1)


    val numCluster = 7
    //最大的分类数(设置多少个中心点，也是KMeans中的K)
    val numTerations = 50 //最大的迭代次数
    val clusters: KMeansModel = KMeans.train(train_date, numCluster, numTerations) //训练模型


    val company_date = res_k_means.randomSplit(Array(0.8, 0.2), 11L)(1)
    val scaler_company = new StandardScaler(withMean = true, withStd = true).fit(company_date)
    company_date.map(x => {
      //用模型对这个标准化后的数据进行预测|原始数据|标准化后的数据
      //分类是从0开始的
      s"${clusters.predict(scaler_company.transform(x)) + 1}\t${x.toArray.mkString("\t")}"
      //      (clusters.predict(scaler_company.transform(x)) + 1, x, scaler_company.transform(x))
    }).foreach(println(_))
    //      .repartition(1).saveAsTextFile("F:\\tmp\\company\\ccA")

    //找到最优的分类
    //        Optimal_classification(train_date: RDD[linalg.Vector], test_date: RDD[linalg.Vector])

    sc.stop()
  }
}
