package com.bzn.model

import com.bzn.sparkUtil.SparkUtil
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext

/**
  * author:xiaoYuanRen
  * Date:2019/4/28
  * Time:14:25
  * describe: kmeans 算法训练模型
  **/
object KmeansDemoTest extends SparkUtil{
  def main(args: Array[String]): Unit = {
    val appName = this.getClass.getName
    val sparkInfo = sparkConfInfo(appName,"")
    val conf = sparkInfo._1
    val sc = sparkInfo._2
    val hiveContext = sparkInfo._4

    kmeansModel(hiveContext)
  }

  /**
    * kmeans 训练模型的方法
    * @param hiveContext
    */
  def kmeansModel(hiveContext:HiveContext)={
    //读取训练数据
    val modelData = hiveContext.sql("select * from personal_model_data")

    //将训练的数据过滤无效字段，并转换为向量形式
    val modelDataVetor: RDD[linalg.Vector] = modelData.map(x => {
      val modelData = x.mkString("\u0001").split("\u0001").toBuffer
      //去掉cert_no 和ent_id
      modelData.remove(7,2)
      val ArrayModelData = modelData.map(_.toDouble).toArray
      Vectors.dense(ArrayModelData)
    })

    //特征向量标准化处理
    //withMean：默认为假。此种方法将产出一个稠密输出，所以不适用于稀疏输入。
    //withStd：默认值为真，使用统一标准差方式。
    //withMean 如果值为true，那么将会对列中每个元素减去均值（否则不会减）
    //withStd 如果值为true，那么将会对列中每个元素除以标准差（否则不会除，这个值一般为 true，否则没有标准化没有意义）
    //所以上面两个参数都为 false 是没有意义的，模型什么都不会干，返回原来的值，这些将会在下面的代码中得到验证。
    val standardData = new StandardScaler(withMean = true,withStd = true ).fit(modelDataVetor).transform(modelDataVetor)

    //将训练数据随机取0.8作为训练，0.2作为测试
    val randomData = standardData.randomSplit(Array(0.8,0.2),2L)
    val trainData: RDD[linalg.Vector] = randomData(0)
    val testData = randomData(1)

    //参数调优
    modelNumCluster(trainData,testData)

    //设置中心数
    val numCluster = 5
    //设置最大迭代次数
    val numTerations = 50
    //使用kmeans算法训练模型
//    val kmeansModel = KMeans.train(trainData,numCluster,numTerations)

    //使用测试数据进行测试
//    testData.map(x => {
//      kmeansModel.predict(x)
//    }).foreach(println)
  }

  /**
    * 找到最优分类，使用过key均值交叉验证
    * K-均值在交叉验证的情况，WCSS随着K的增大持续减小，但是达到某个值后，下降的速率突然会变得很平缓。
    * 这时的K通常为最优的K值（这称为拐点）。k最佳为10左右，
    * 尽管较大的K值从数学的角度可以得到更优的解，但是类簇太多就会变得难以理解和解释
    * @param trainData
    */
  def modelNumCluster(trainData:RDD[linalg.Vector],testData:RDD[linalg.Vector]) = {
    val numTerations = 50
    val numClustersPredictData = Seq(1,2,3,4,5,6,7,8,9,10,11,12,13,15,16,17,19,20).map(numCluster => {
      (numCluster,KMeans.train(trainData,numCluster,numTerations).computeCost(testData))
    })

    numClustersPredictData.foreach(println)
  }
}
