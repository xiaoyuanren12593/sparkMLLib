package value

import java.text.NumberFormat
import java.util.Properties

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import risk__level.Get_configure


/**
  * Created by MK on 2018/5/8.
  */
object Create_enterValue_model {

  //调优
  def tuning(): Unit = {
    //对数据进行归一化处理
    //    val vectors = labeled_file.map(x => (x.label, x.features)).toDF("label", "features")
    //    val scaler_M = new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures")
    //    val scalerModel = scaler_M.fit(vectors)
    //    val scaledData = scalerModel.transform(vectors)
    //    val file_tree = scaledData.map(x => LabeledPoint(x.getAs("label"), x.getAs("scaledFeatures")))

    //进行标准化处理
    //    val vectors = labeled_file.map(x => (x.label, x.features)).toDF("label", "features")
    //    val scaler = new StandardScaler()
    //      .setInputCol("features")
    //      .setOutputCol("scaledFeatures")
    //      .setWithStd(true)
    //      .setWithMean(true)
    //    val scalerModel = scaler.fit(vectors)
    //    val scaledData = scalerModel.transform(vectors)

    //正则化默认是L2,下面采用的是L1
    /**
      * 线性回归同样可以采用正则化手段，其主要目的就是防止过拟合。
      * *
      * 当采用L1正则化时，则变成了Lasso Regresion；当采用L2正则化时，则变成了Ridge Regression；
      * 线性回归未采用正则化手段。通常来说，在训练模型时是建议采用正则化手段的，特别是在训练数据的量特别少的时候，若不采用正则化手段，
      * 过拟合现象会非常严重。L2正则化相比L1而言会更容易收敛（迭代次数少），但L1可以解决训练数据量小于维度的问题（也就是n元一次方程只有不到n个表达式，这种情况下是多解或无穷解的）。
      **/

    //    val normalizer = new Normalizer().setInputCol("features").setOutputCol("normFeatures").setP(1.0)
    //    val l1NormData = normalizer.transform(vectors)
    //    val file_tree = l1NormData.map(x => LabeledPoint(x.getAs("label"), x.getAs("normFeatures")))
    //
    //    val tree_orgin = file_tree.randomSplit(Array(0.8, 0.5), 11L)
    //    val tree_train = tree_orgin(0).cache()
    //    val tree_test = tree_orgin(1)
    //    //
    //    val model = LinearRegressionWithSGD.train(tree_train, 20, 0.5)
    //    tree_test.map(x => {
    //      s"预测值:${model.predict(x.features)},实际值:${x.label}"
    //    }).foreach(println(_))


    //获取“正确答案y的值和x1，x2的值”
    //    val Step_Results = Seq(0.01, 0.025, 0.05, 0.1, 0.12, 0.15, 0.5, 0.75, 1.0, 1.02, 1.05, 1.5, 1.75, 2.0, 2.02, 2.05, 2.5, 2.75, 3.0).map(param => {
    //      val model = LinearRegressionWithSGD.train(tree_train, 20, param)
    //      val valueAndPreds = tree_test.map { point =>
    //        val pointLabel = point.label //找出“正确答案”种x1,x2对应的真正的y值
    //      val prediction = model.predict(point.features) //将“正确答案”的x1，x2的值带入模型中，并预测出y的值
    //        (pointLabel, prediction)
    //      }
    //      val MSE = valueAndPreds.map { case (v, p) => math.pow(v - p, 2) }.mean()
    //      (s"$param lambda", MSE)
    //    })
    //    //    /*计算MSE的结果输出*/
    //    Step_Results.foreach { case (param, mse) => println(f"$param, mse = $mse") }

    //    val Step_Results = Seq(1, 5, 10, 20, 50, 100).map(param => {
    //      val model = LinearRegressionWithSGD.train(tree_train, param, 0.45)
    //      val valueAndPreds = tree_test.map { point =>
    //        val pointLabel = point.label //找出“正确答案”种x1,x2对应的真正的y值
    //      val prediction = model.predict(point.features) //将“正确答案”的x1，x2的值带入模型中，并预测出y的值
    //        (pointLabel, prediction)
    //      }
    //      val MSE = valueAndPreds.map { case (v, p) => math.pow(v - p, 2) }.mean()
    //      (s"$param lambda", MSE)
    //    })
    //    //    /*计算MSE的结果输出*/
    //    Step_Results.foreach { case (param, mse) => println(f"$param, mse = $mse")
    //    }


    /*调节步长数的大小*/
    //    val Step_Results = Seq(0.01, 0.025, 0.05, 0.1, 0.12, 0.15, 0.5, 0.75, 1.0, 1.02, 1.05, 1.5, 1.75, 2.0, 2.02, 2.05, 2.5, 2.75, 3.0).map { param =>
    //      val model = LinearRegressionWithSGD.train(tree_train, 30, param)
    //      val scoreAndLabels = tree_test.map { point =>
    //
    //        (model.predict(point.features).formatted("%.2f"), point.label.toDouble.formatted("%.2f"))
    //      }
    //      val rmsle = math.sqrt(scoreAndLabels.map(x => math.pow(math.log(x._1.toDouble) - math.log(x._2.toDouble), 2)).mean)
    //      (s"$param lambda", rmsle)
    //    }
    //    /*步长的结果输出*/
    //    Step_Results.foreach { case (param, rmsl) => println(f"$param, rmsle = $rmsl") }


    //    /*调节迭代次数*/
    //        val Iter_Results = Seq(1, 5, 10, 20, 50, 100).map { param =>
    //          val model = LinearRegressionWithSGD.train(tree_train, param, 0.01)
    //          val scoreAndLabels = tree_test.map { point =>
    //            (model.predict(point.features), point.label)
    //          }
    //          val rmsle = math.sqrt(scoreAndLabels.map(x => math.pow(math.log(x._1) - math.log(x._2), 2)).mean)
    //          (s"$param lambda", rmsle)
    //        }
    //        /*迭代次数的结果输出*/
    //        Iter_Results.foreach { case (param, rmsl) => println(f"$param, rmsle = $rmsl")}
  }

  //创建Hbase的RDD
  def Hbase_rdd(sc: SparkContext, config: Get_configure): RDD[(ImmutableBytesWritable, Result)]
  = {
    val zookeeper_url = config.get().getProperty("hbase.zookeeper.quorum")
    //定义HBase的配置
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", zookeeper_url)

    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "labels:label_user_enterprise_vT")

    val usersRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    )
    usersRDD
  }

  //得到该企业的ID，平均增长率，最近3个月的增长率，该企业的总保费
  def Get_enterprise_premium_slope(enterprise_premium_slope: DataFrame): RDD[(String, (String, String, String))]
  = {
    val end: RDD[(String, (String, String, String))] = enterprise_premium_slope.map(x => {
      val ent_id = x.getAs("ent_id").toString
      val slope = x.getAs("slope").toString
      val three_slope = x.getAs("three_slope").toString
      val premium_all = x.getAs("premium_all").toString
      (ent_id, (slope, three_slope, premium_all))
    })
    end
  }

  //得到企业的ID，和当前在保人数
  def Get_enter_people(usersRDD: RDD[(ImmutableBytesWritable, Result)]): RDD[(String, String)]
  = {
    val enter_people: RDD[(String, String)] = usersRDD.map(x => {
      val s: (ImmutableBytesWritable, Result) = x
      //当前在保人数
      val cur_insured_persons = Bytes.toString(s._2.getValue("insureinfo".getBytes, s"cur_insured_persons".getBytes))
      val end = if (cur_insured_persons == null) "0" else cur_insured_persons
      //得到企业的ID
      val kv = s._2.getRow
      val rowkey = Bytes.toString(kv)
      (rowkey, end)
    })
    enter_people
  }

  //得到labelPoint
  def Get_labelPoint(config: Get_configure, sqlContext: HiveContext, tep_One: RDD[(String, (String, String, String))], tep_Two: RDD[(String, String)]): RDD[(String, LabeledPoint)]
  = {
    val numberFormat = NumberFormat.getInstance
    // 设置精确到小数点后2位
    numberFormat.setMaximumFractionDigits(2)

    /**
      * 链接mysql读取包含企业价值的表，从中读取分数
      **/
    val url = config.get().getProperty("mysql_url")
    val prop = new Properties()
    val tepThree = sqlContext.read.jdbc(url, "ent_risk_value", prop).map(x => {
      val ent_id = x.getAs("ent_id").toString
      val ent_Value_Score = x.getAs("EntValueScore").toString
      (ent_id, ent_Value_Score)
    })

    val labeled_file: RDD[(String, LabeledPoint)] = tep_One.join(tep_Two).map(x => {
      val ent_id = x._1
      val slope = x._2._1._1.replaceAll("%", "")
      val three_slope = x._2._1._2.replaceAll("%", "")
      val premium_all = x._2._1._3.toFloat
      val end_premium_all = numberFormat.format(premium_all).replaceAll(",", "")
      val people = x._2._2
      (ent_id, (slope, three_slope, end_premium_all, people))
    }).join(tepThree).map(x => {
      val ent_id = x._1
      //1,128.39
      val slope = x._2._1._1.replaceAll(",", "").toDouble
      val three_slope = x._2._1._2.toDouble
      val end_premium_all = x._2._1._3.replace(",", "").toDouble
      val people = x._2._1._4.toDouble
      val ent_Value_Score = x._2._2.toDouble
      val train = Array(slope, three_slope, end_premium_all, people)
      //      val train = Array(slope / 100, three_slope / 100)
      (ent_id, LabeledPoint(ent_Value_Score.toDouble, Vectors.dense(train)))
    }).cache()

    labeled_file
  }

  //计算真实评分
  def get_Score(people: Double, end_premium_all: Double, three_slope: Double): (String, String, String)
  = {
    //计算人数评分
    val people_score = if (people >= 0 && people < 1000) {
      val p = (people - 0.0) / 1000.0
      val score_people = 30.0 * p + 0.0
      score_people.formatted("%.2f")
    } else if (people >= 1000 && people < 5000) {
      val p = (people - 1000.0) / (5000.0 - 1000.0)
      val score_people = 20.0 * p + 30.0
      score_people.formatted("%.2f")
    } else if (people >= 5000 && people < 10000) {
      val p = (people - 5000.0) / (10000.0 - 5000.0)
      val score_people = 30.0 * p + 50.0
      score_people.formatted("%.2f")
    } else if (people >= 10000 && people < 50000) {
      val p = (people - 10000.0) / (50000.0 - 10000.0)
      val score_people = 20.0 * p + 80.0
      score_people.formatted("%.2f")
    } else "0"
    //计算保费评分
    val end_premium_all_score = if (end_premium_all >= 0 && end_premium_all < 100000) {
      val p = (end_premium_all - 0.0) / 100000.0
      val score_premium = 50.0 * p + 0.0
      score_premium.formatted("%.2f")
    } else if (end_premium_all >= 100000 && end_premium_all < 600000) {
      val p = (end_premium_all - 100000.0) / 500000.0
      val score_premium = 30.0 * p + 50.0
      score_premium.formatted("%.2f")
    } else if (end_premium_all >= 600000 && end_premium_all < 2000000) {
      val p = (end_premium_all - 600000.0) / 1400000.0
      val score_premium = 20.0 * p + 80.0
      score_premium.formatted("%.2f")
    } else "0"
    //最近3个月的变化率评分
    val three_slope_scope = if (three_slope < 0) {
      "0"
    } else if (three_slope >= 0 && three_slope < 50) {
      val p = (three_slope - 0.0) / 50.0
      val three_slope_score = 50.0 * p + 0.0
      three_slope_score.formatted("%.2f")
    } else if (three_slope >= 50 && three_slope < 100) {
      val p = (three_slope - 50.0) / 50.0
      val three_slope_score = 50.0 * p + 50.0
      three_slope_score.formatted("%.2f")
    } else if (three_slope >= 100) {
      "100"
    }

    //权重
    val people_end = people_score.toDouble * 0.3
    val end_premium_all_score_end = end_premium_all_score.toDouble * 0.35
    val three_slope_scope_end = three_slope_scope.toString.toDouble * 0.35

    (people_end.toString, end_premium_all_score_end.toString, three_slope_scope_end.toString)
  }

  //通过人数评分，保费评分，三月变化率评分，求得我该企业的分数
  def get_labeled_file_before(config: Get_configure, sqlContext: HiveContext, tep_One: RDD[(String, (String, String, String))], tep_Two: RDD[(String, String)]): RDD[(String, String, Double, Double, Double)]
  = {
    val end: RDD[(String, String, Double, Double, Double)] = Get_labelPoint(config, sqlContext, tep_One, tep_Two).map(s => {
      //得到labelPoint
      val x = s._2
      val train = x.features.toArray
      val slope = train(0)
      val three_slope = train(1)
      val end_premium_all = train(2)
      val people = train(3)

      val end = get_Score(people, end_premium_all, three_slope)
      //人数评分，保费评分，三月变化率评分 (求和)
      val ar_end = Array(end._1, end._2, end._3).map(_.toDouble).sum.formatted("%.2f")
      (s._1, ar_end, people, end_premium_all, three_slope)
    })
    end
  }

  def main(args: Array[String]): Unit = {
    val config: Get_configure = new Get_configure

    val conf_spark = new SparkConf().setAppName("wuYu").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable])).set("spark.sql.broadcastTimeout", "36000")
      .setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf_spark)
    //城市编码
    val sqlContext: HiveContext = new HiveContext(sc)
    val enterprise_premium_slope: DataFrame = sqlContext.sql("select * from odsdb_prd.enterprise_premium_slope")


    //创建Hbase的RDD
    val usersRDD: RDD[(ImmutableBytesWritable, Result)] = Hbase_rdd(sc, config: Get_configure)

    //得到该企业的ID，平均增长率，最近3个月的增长率，该企业的总保费
    val tep_One: RDD[(String, (String, String, String))] = Get_enterprise_premium_slope(enterprise_premium_slope: DataFrame).cache

    //得到企业的ID，和当前在保人数
    val tep_Two: RDD[(String, String)] = Get_enter_people(usersRDD: RDD[(ImmutableBytesWritable, Result)]).cache

    //ent_id | 价值分 | 在保人数 | 累计保费(截至当当前月) | 3月变化率
    //    (3b9173bf3e1c469496c0fa5d670c6912,3.21,35.0,9505.08,3.51)
   val model_save =  get_labeled_file_before(config, sqlContext, tep_One, tep_Two)
//    model_save.take(10).foreach(println)
  }
}
