package company.sparkMLlib.ofo_person_Analysis

import com.alibaba.fastjson.JSON
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext, mllib}

/**
  * Created by MK on 2018/6/22.
  */
object Analysis_model {
  //找到最优的分类
  def Optimal_classification(train_date: RDD[mllib.linalg.Vector], test_date: RDD[mllib.linalg.Vector]): Unit
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


  val conf_spark = new SparkConf()
    .setAppName("wuYu")
  conf_spark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf_spark.registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
  conf_spark.set("spark.sql.broadcastTimeout", "36000")
    .setMaster("local[2]")

  /**
    * 第一步:创建一个JobConf
    **/
  //定义HBase的配置
  val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  conf.set("hbase.zookeeper.quorum", "172.16.11.106")

  //设置查询的表名
  conf.set(TableInputFormat.INPUT_TABLE, "labels:label_user_personal_vT")


  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val sc = new SparkContext(conf_spark)

    val sqlContext: HiveContext = new HiveContext(sc)
    val usersRDD = sc.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    )

    //得到省份
    //d_cant	省市区码表	省市区信息
    val d_cant = sqlContext.sql("select * from odsdb_prd.d_cant").filter("length(name)>0").cache

    //通过省区的名字，找出对应的code
    val cert_native_province_r = d_cant.map(x => {
      val code = x.getString(0)
      val name = x.getString(1)
      (name, code)
    }).filter(_._2.contains("0000")).collectAsMap


    import sqlContext.implicits._
    val result = sc.textFile("F:\\tmp\\ofo\\ofo_model_date.txt")
      .map(x => {
        val insure_no = x.substring(0, 19).replace(",", "")
        val ofo_value = x.substring(19, x.length)

        val sum_value = JSON.parseObject(ofo_value)
        //年龄
        val age = sum_value.getString("user_information").split(",")(1)
        val province = sum_value.getString("user_information").split(",")(3)
        //省份代号
        val get_province_code = cert_native_province_r.getOrElse(province, "0")
        //投保次数
        val user_insure_product_num = sum_value.getString("user_insure_product_num")

        //不管是早起族还是夜猫子只要是有值，那么就将其归为1，没值为0
        val early_night = sum_value.getString("early_night")
        val early_night_end = if (early_night == "null") 0.0 else 1.0
        val end_value = Array(age, get_province_code, user_insure_product_num, early_night_end).map(_.toString.toDouble)
        (insure_no, Vectors.dense(end_value), province)
      }).toDF("label", "features", "province")

    val scaler_M = new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures")
    val scalerModel = scaler_M.fit(result)
    val scaledData = scalerModel.transform(result).map(x => x.getAs[org.apache.spark.mllib.linalg.Vector]("scaledFeatures"))

    val numCluster = 7
    //        最大的分类数(设置多少个中心点，也是KMeans中的K)
    val numTerations = 50 //最大的迭代次数
    val clusters: KMeansModel = KMeans.train(scaledData, numCluster, numTerations) //训练模型


    clusters.save(sc, "hdfs://namenode1.cdh:8020/model/only_ofo")

    scalerModel.transform(result).map(x => {
      val scaledFeatures = x.getAs[mllib.linalg.Vector]("scaledFeatures")
      val features = x.getAs[mllib.linalg.Vector]("features")
      val label = x.getAs[String]("label")
      val province = x.getAs[String]("province")
      val predict_result = clusters.predict(scaledFeatures)
      (label, features, predict_result, province)
    }).repartition(1).saveAsTextFile("C:\\Users\\a2589\\Desktop\\ofo_model_end.txt")


    //    val all = scaledData.randomSplit(Array(0.8, 0.2), 11L)
    //    val train_date = all(0).cache
    //    val test_date = all(1)
    //    Optimal_classification(train_date: RDD[mllib.linalg.Vector], test_date: RDD[mllib.linalg.Vector])

  }
}
