package com.bzn.model

import com.bzn.sparkUtil.SparkUtil
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


object DecisionTreeDemoTest extends SparkUtil{
  def main(args: Array[String]): Unit = {

    val appName = this.getClass.getName

    //读取配置信息
    val sparkInfo: (SparkConf, SparkContext, SQLContext, HiveContext) = sparkConfInfo(appName,"")
    val conf = sparkInfo._1
    val sc = sparkInfo._2
    val hiveContest = sparkInfo._4

    //训练模型
    trainModel(hiveContest)

    sc.stop()
  }

  /**
    * 训练模型
    * @param hiveContest
    */
  def trainModel(hiveContest:HiveContext)= {
    //读取训练数据
    val trainDataFromHive = hiveContest.sql("select * from enter_model_data")

    import hiveContest.implicits._
    val resData = trainDataFromHive.map(x => {
      val dataList = x.mkString("\u0001").split("\u0001").map(_.toDouble).toBuffer
      dataList.remove(14)
      val riskLevel = x.getAs[String]("risk_level").toDouble
      LabeledPoint(riskLevel,Vectors.dense(dataList.toArray))
    })

    val vectors: DataFrame = resData.map(x => (x.label,x.features)).toDF("label","features")

    //变准化
    val standardScaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithMean(true)
      .setWithStd(true)

    //计算标准化模型
    val fitVector = standardScaler.fit(vectors)

    //转换
//    fitVector.transform(vectors).printSchema()
    val transformeVector: RDD[LabeledPoint] = fitVector.transform(vectors)
      .map(x => LabeledPoint(x.getAs("label"),x.getAs("scaledFeatures")))

    //将历史数据随机分为两部分，一部分用作训练，一部分用作测试
    val randomData = transformeVector.randomSplit(Array(0.8,0.2),2)
    val trainData: RDD[LabeledPoint] = randomData(0)
    val testData = randomData(1)

    //模型调参
    //深度调参
    modelMaxDeep(trainData,testData)
    //分裂数调参
    modelMaxBins(trainData,testData)

    // 通过模型调优，训练模型 深度是4 最大分裂数16
    val numClasses = 4 //分类数
    val categoricalFeaturesInfo = Map[Int,Int]()
    val impurity = "entropy"  //增量熵
    val maxDepth = 4
    val maxBins = 16
//    val decisionTreeModel = DecisionTree.trainClassifier(trainData,numClasses,categoricalFeaturesInfo,impurity,maxDepth,maxBins)

//    println(decisionTreeModel.toDebugString)
//    testData.map(point => {
//      (decisionTreeModel.predict(point.features),point.label)
//    }).foreach(println)

  }

  /**
    *
    * @param trainData 训练数据集
    * @param testData  测试数据集
    */
  def modelMaxDeep(trainData:RDD[LabeledPoint],testData:RDD[LabeledPoint]) ={
    val numClasses = 4 //分类数
    val categoricalFeaturesInfo = Map[Int,Int]()
    val impurity = "entropy" //信息熵
    val maxBins = 16
    val res = Seq(1,2,3,4,5,6,7,9,10,12,15).map(maxDepth => {
      //传入不同的深度，得到不同的模型
      val testModel = DecisionTree.trainClassifier(trainData,numClasses,categoricalFeaturesInfo,impurity,maxDepth,maxBins)
      //使用测试数据，得到预测结果和真正的结果进行比较
      val resPredictData = testData.map(x => {
        (testModel.predict(x.features),x.label)
      })
      //使用均方差验证，不同深度的结果值
      /**
        *  sqrt 返回正确舍入的 double 值的正平方根。
        *  pow  返回第一个参数的第二个参数次幂的值。
        *  log 返回 double 值的自然对数（底数是 e）。
        *  mean均值，variance方差，stddev标准差，corr(Pearson相关系数)，skewness偏度，kurtosis峰度。
        */
      val sqrtData = math.sqrt(resPredictData.map(x => {
        math.pow(math.log(x._1)-math.log(x._2),2)
      }).mean())

      (maxDepth,sqrtData)
    })
    res.foreach(x => {
      println("深度="+x._1+"均方差结果为"+x._2)
    })
  }

  def modelMaxBins(trainData:RDD[LabeledPoint],testData:RDD[LabeledPoint]) = {
    val numClasses = 4 //分类数
    val categoricalFeaturesInfo = Map[Int,Int]()
    val impurity = "entropy" //信息熵
    val maxDepth = 4
    val res = Seq(2,4,6,8,10,12,16,20,32,64,100,200).map(maxBins=>{
      val testModel = DecisionTree.trainClassifier(trainData,numClasses,categoricalFeaturesInfo,impurity,maxDepth,maxBins)
      val predictData = testData.map(x=> {
        (testModel.predict(x.features),x.label)
      })
      val sqrtData = math.sqrt(predictData.map(x => {
        math.pow(math.log(x._1-math.log(x._2)),2)
      }).mean())
      (maxBins,sqrtData)
    })

    res.foreach(x => {
      println("分裂数="+x._1+"均方差结果为"+x._2)
    })
  }
}
