//package xing
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql._
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
//import org.apache.spark.sql.functions._
//import org.apache.spark.ml.Pipeline
//import org.apache.spark.ml.classification.DecisionTreeClassificationModel
//import org.apache.spark.ml.classification.DecisionTreeClassifier
//import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
//import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
//
//
//object DecisionTreeClassifierTest {
////  val spark = SparkSession.builder().appName("Spark decision tree classifier").config("spark.some.config.option", "some-value").getOrCreate()
//  val sparkConf = new SparkConf().setAppName(DecisionTreeClassifierTest.getClass.getName).setMaster("local[*]")
//  // For implicit conversions like converting RDDs to DataFrames
//  val sc = new SparkContext(sparkConf)
//
//  val ssc = new SQLContext(sc)
//
//  import ssc.implicits._
//  // 这里仅仅是示例数据，完整的数据源，请参考我的博客http://blog.csdn.net/hadoop_spark_storm/article/details/53412598
//  val dataList: List[(Double, String, Double, Double, String, Double, Double, Double, Double)] = List(
//    (0, "male", 37, 10, "no", 3, 18, 7, 4),
//    (0, "female", 27, 4, "no", 4, 14, 6, 4),
//    (0, "female", 32, 15, "yes", 1, 12, 1, 4),
//    (0, "male", 57, 15, "yes", 5, 18, 6, 5),
//    (0, "male", 22, 0.75, "no", 2, 17, 6, 3),
//    (0, "female", 32, 1.5, "no", 2, 17, 5, 5))
//
//  val data = dataList.toDF("affairs", "gender", "age", "yearsmarried", "children", "religiousness", "education", "occupation", "rating")
//
//  data.createOrReplaceTempView("data")
//
//  // 字符类型转换成数值
//  val labelWhere = "case when affairs=0 then 0 else cast(1 as double) end as label"
//  val genderWhere = "case when gender='female' then 0 else cast(1 as double) end as gender"
//  val childrenWhere = "case when children='no' then 0 else cast(1 as double) end as children"
//
//  val dataLabelDF = spark.sql(s"select $labelWhere, $genderWhere,age,yearsmarried,$childrenWhere,religiousness,education,occupation,rating from data")
//
//  val featuresArray = Array("gender", "age", "yearsmarried", "children", "religiousness", "education", "occupation", "rating")
//
//  // 字段转换成特征向量
//  val assembler = new VectorAssembler().setInputCols(featuresArray).setOutputCol("features")
//  val vecDF: DataFrame = assembler.transform(dataLabelDF)
//  vecDF.show(10,truncate=false)
//
//  // 索引标签，将元数据添加到标签列中
//  val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(vecDF)
//  labelIndexer.transform(vecDF).show(10,truncate=false)
//
//  // 自动识别分类的特征，并对它们进行索引
//  // 具有大于8个不同的值的特征被视为连续。
//  val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(8).fit(vecDF)
//  featureIndexer.transform(vecDF).show(10,truncate=false)
//
//  // 将数据分为训练和测试集（30%进行测试）
//  val Array(trainingData, testData) = vecDF.randomSplit(Array(0.7, 0.3))
//
//  // 训练决策树模型
//  val dt = new DecisionTreeClassifier()
//    .setLabelCol("indexedLabel")
//    .setFeaturesCol("indexedFeatures")
//    .setImpurity("entropy") // 不纯度
//    .setMaxBins(100) // 离散化"连续特征"的最大划分数
//    .setMaxDepth(5) // 树的最大深度
//    .setMinInfoGain(0.01) //一个节点分裂的最小信息增益，值为[0,1]
//    .setMinInstancesPerNode(10) //每个节点包含的最小样本数
//    .setSeed(123456)
//
//  // 将索引标签转换回原始标签
//  val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
//
//  // Chain indexers and tree in a Pipeline.
//  val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))
//
//  // Train model. This also runs the indexers.
//  val model = pipeline.fit(trainingData)
//
//  // 作出预测
//  val predictions = model.transform(testData)
//
//  // 选择几个示例行展示
//  predictions.select("predictedLabel", "label", "features").show(10,truncate=false)
//
//  // 选择（预测标签，实际标签），并计算测试误差。
//  val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")
//  val accuracy = evaluator.evaluate(predictions)
//  println("Test Error = " + (1.0 - accuracy))
//
//  // 这里的stages(2)中的“2”对应pipeline中的“dt”，将model强制转换为DecisionTreeClassificationModel类型
//  val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
//  treeModel.getLabelCol
//  treeModel.getFeaturesCol
//  treeModel.featureImportances
//  treeModel.getPredictionCol
//  treeModel.getProbabilityCol
//
//  treeModel.numClasses
//  treeModel.numFeatures
//  treeModel.depth
//  treeModel.numNodes
//
//  treeModel.getImpurity
//  treeModel.getMaxBins
//  treeModel.getMaxDepth
//  treeModel.getMaxMemoryInMB
//  treeModel.getMinInfoGain
//  treeModel.getMinInstancesPerNode
//
//  // 查看决策树
//  println("Learned classification tree model:\n" + treeModel.toDebugString)
//}
