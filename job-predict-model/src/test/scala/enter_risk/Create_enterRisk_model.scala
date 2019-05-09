package enter_risk

import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MK on 2018/4/18.
  */
object Create_enterRisk_model {
  //调节树的深度参数
  def Deep_Results(train_data: RDD[LabeledPoint], categoricalFeaturesInfo: Map[Int, Int], test_data: RDD[LabeledPoint]): Unit
  = {
    /*调节树深度次数*/
    val Deep_Results = Seq(1, 2, 3, 4, 5, 10, 20).map { param =>
      val model = DecisionTree.trainClassifier(train_data, 4, categoricalFeaturesInfo, "entropy", param, 32)
      val scoreAndLabels = test_data.map { point =>
        (model.predict(point.features), point.label)
      }
      val rmsle = math.sqrt(scoreAndLabels.map(x => math.pow(math.log(x._1) - math.log(x._2), 2)).mean)
      (s"$param lambda", rmsle)
    }
    /*深度的结果输出*/
    Deep_Results.foreach { case (param, rmsl) => println(f"$param, rmsle = ${rmsl}") }

  }

  //调节树的min（划分数）
  def ClassNum_Results(train_data: RDD[LabeledPoint], categoricalFeaturesInfo: Map[Int, Int], test_data: RDD[LabeledPoint]): Unit
  = {
    /*调节划分数*/
    val ClassNum_Results = Seq(2, 4, 8, 16, 32, 64, 100).map { param =>
      val model = DecisionTree.trainClassifier(train_data, 4, categoricalFeaturesInfo, "entropy", 10, param)
      val scoreAndLabels = test_data.map { point =>
        (model.predict(point.features), point.label)
      }
      val rmsle = math.sqrt(scoreAndLabels.map(x => math.pow(math.log(x._1) - math.log(x._2), 2)).mean)
      (s"$param lambda", rmsle)
    }
    /*划分数的结果输出*/
    ClassNum_Results.foreach { case (param, rmsl) => println(f"$param, rmsle = ${rmsl}") }

  }

  //MSE均方误差
  def mse_Model(test_data: RDD[LabeledPoint], model_DT: DecisionTreeModel): Unit
  = {
    val predict_vs_train = test_data.map {
      point => (model_DT.predict(point.features), point.label)
      /* point => (math.exp(model_DT.predict(point.features)), math.exp(point.label))*/
    }
    //    predict_vs_train.take(5).foreach(println(_))
    /*MSE是均方误差*/
    val mse = predict_vs_train.map(x => math.pow(x._1 - x._2, 2)).mean()
    /* 平均绝对误差（MAE）*/
    //    val mae = predict_vs_train.map(x => math.abs(x._1 - x._2)).mean()
    /*均方根对数误差（RMSLE）*/
    //    val rmsle = math.sqrt(predict_vs_train.map(x => math.pow(math.log(x._1 + 1) - math.log(x._2 + 1), 2)).mean())
    //    println(s"mse is $mse and mae is $mae and rmsle is $rmsle")
    println(s"mse is $mse")
  }

  //得到labelPoint并进行标准化,建立模型
  def get_label_point(enter_model_data: DataFrame, sqlContext: HiveContext): DecisionTreeModel
  = {
    val file_tree: RDD[LabeledPoint] = enter_model_data.map(x => {
      //对hive中的row直接进行操作
      val s = x.mkString(",").split(",").map(_.toDouble).toBuffer
      //以下标删除数组中的风险等级
      s.remove(14)
      val risk_level = x.getAs("risk_level").toString.toDouble
      LabeledPoint(risk_level, Vectors.dense(s.toArray))
    }).cache()

    //标准化
    import sqlContext.implicits._
    val vectors = file_tree.map(x => (x.label, x.features)).toDF("label", "features")
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(true)
    val scalerModel = scaler.fit(vectors)

    val train_data = scalerModel.transform(vectors).map(x => LabeledPoint(x.getAs("label"), x.getAs("scaledFeatures")))


    val numClasses = 4 //设定分类数量
    val categoricalFeaturesInfo = Map[Int, Int]() //设定输入格式
    val impurity = "entropy" //以信息熵增益的方式进行计算
    val maxDepth = 4 //设定树的高度
    val maxBins = 16 //每个特征分裂时，最大的属性数目,离散化"连续特征"的最大划分数

    val model_DT: DecisionTreeModel = DecisionTree.trainClassifier(train_data
      , numClasses
      , categoricalFeaturesInfo
      , impurity
      , maxDepth, maxBins) //建立模型
    Deep_Results(train_data: RDD[LabeledPoint], categoricalFeaturesInfo: Map[Int, Int], train_data: RDD[LabeledPoint])
    ClassNum_Results(train_data: RDD[LabeledPoint], categoricalFeaturesInfo: Map[Int, Int], train_data: RDD[LabeledPoint])
    model_DT

  }

  //模型保存与加载
  def model_save_load(model_DT: DecisionTreeModel, sc: SparkContext): Unit
  = {
//    val test = Vectors.dense(0, 0, 30, 1.2, 1.3, 1000, 73, 28, 39, 1, 4, 300, 12, 360, 63, 0.01, 4, 0, 100.47, 2175143.47, 3307314, 9, 60, 500000)
    val test = Vectors.dense(4,61,99.75,0.25, 42.63,1,0,3,172.67,9862, 330400,0,808000,24624,3)
    val modelPath = "C:\\Users\\xingyuan\\Desktop\\未完成\\7.机器学习-和赔模型\\tmp\\company\\model"
    model_DT.save(sc, "C:\\Users\\xingyuan\\Desktop\\未完成\\7.机器学习-和赔模型\\tmp\\company\\model")
    val modelLoad = DecisionTreeModel.load(sc, modelPath)
    val end = modelLoad.predict(test)
    println(end)
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")

    val conf_spark = new SparkConf().setAppName("wuYu").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf_spark)
    //城市编码
    val sqlContext: HiveContext = new HiveContext(sc)
    val enter_model_data: DataFrame = sqlContext.sql("select * from enter_model_data").cache()

    //得到labelPoint并进行标准化,建立模型 ，注意的是，如果要预测的话需要对其进行标准化
    val model_DT = get_label_point(enter_model_data: DataFrame, sqlContext: HiveContext)
    println("Learned classification tree model:\n" + model_DT.toDebugString)
    //模型保存
//    model_DT.save(sc, "hdfs://namenode1.cdh:8020/model/enter_risk")


    //模型参数调优:调节树的深度参数
    //Deep_Results(train_data: RDD[LabeledPoint], categoricalFeaturesInfo: Map[Int, Int], test_data: RDD[LabeledPoint])
    //调节树的min（划分数）
    //        ClassNum_Results(train_data: RDD[LabeledPoint], categoricalFeaturesInfo: Map[Int, Int], test_data: RDD[LabeledPoint])

    //模型保存与加载
//    model_save_load(model_DT: DecisionTreeModel, sc: SparkContext)
    sc.stop()
  }
}
