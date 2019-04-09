package com.bzn.model

import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * mk xingyuan
  * date 2019-3-29
  */
object ModelLoadTest {
  val conf_spark: SparkConf = new SparkConf().setAppName("wuYu")
  conf_spark.set("spark.sql.broadcastTimeout", "36000").setMaster("local[2]")

  val sc = new SparkContext(conf_spark)
  val sQLContext = new HiveContext(sc)

  def main(args: Array[String]): Unit = {
    //企业风险模型加载
    val modelPath = "hdfs://namenode1.cdh:8020/model/enter_risk"
    val modelLoad = DecisionTreeModel.load(sc, modelPath)
    val rdd = sc.parallelize(Array(Array(1, 5000, 68.71, 31.29, 28.97, 0, 0, 0, 0, 0, 0, 0, 0, 41360, 1),Array (1, 50, 68.71, 31.29, 28.97, 3, 39, 0, 0, 0, 0, 0, 45621, 41360, 5)))
    val vector = rdd.map(x => Vectors.dense(x))

    val standardizer = new StandardScaler(true, true)
    val model = standardizer.fit(vector).transform(vector)
    model.map(x=>{
      modelLoad.predict(x)
    }).foreach(println(_))




    //    val value = Vectors.dense(Array(1, 0, 68.71, 31.29, 28.97, 0, 0, 0, 0, 0, 0, 0, 0, 41360, 1).map(_.toDouble))

    //    val scaler=new StandardScaler(withMean=true,withStd=true).fit(va)//标准化

    //    val end = modelLoad.predict(value)

    //


  }
}
