package company.sparkMLlib.enter_risk

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MK on 2018/4/18.
  */
object read_dataTo_hive {
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
    //3为最佳
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


  //得到城市的编码
  def city_code(sqlContext: HiveContext): collection.Map[String, String]
  = {
    val d_city_grade = sqlContext.sql("select * from odsdb_prd.d_city_grade").select("city_name", "city_code").filter("length(city_name)>1 and city_code is not null")
    val d_city_grade_map: collection.Map[String, String] = d_city_grade.map(x => {
      val city_name = x.getAs("city_name").toString
      val city_code = x.getAs("city_code").toString
      (city_name, city_code)
    }).collectAsMap()
    d_city_grade_map
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf_spark = new SparkConf().setAppName("wuYu")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setMaster("local[2]")
    val sc = new SparkContext(conf_spark)
    //城市编码
    val sqlContext = new HiveContext(sc)
    val d_city_grade_map = city_code(sqlContext)

    //hdfs中的原始数据可以删除企业的品牌影响力
    //工种1	工种2	工种3	工种4	工种5	当前在保	男生占比	女生占比	员工平均年龄	死亡案件数	伤残案件数	工作期间案件数	非工作案件数	报案数	平均出险周期	重大案件率	企业品牌影响力	企业的潜在人员规模     城市名称  	实际赔付额度	预估赔付额度	已赚保费	实际/已赚比	预估/已赚比	 ，风险等级（1-3级）
    val rdd = sc.textFile("F:\\tmp\\company\\enter\\enterprise_Risk.txt")

    import sqlContext.implicits._
    rdd.map(x => {
      val all = x.split("\t")
      val work_level_array = Array(all(0), all(1), all(2), all(3), all(4))
      val work_type_number = work_level_array.zipWithIndex
      //哪个工种的占比最多，就找出对应的工种
      val work_type = work_type_number.map(x => {
        (x._1.replaceAll("%", "").toDouble, x._2 + 1)
      }).reduce((x1, x2) => {
        if (x1._1 >= x2._1) x1 else x2
      })._2 + ""

      //当前在保人数
      val protect_number = all(5).toInt + ""

      //男生占比(%)
      val man_number = all(6).replaceAll("%", "").toDouble + ""
      //女生占比(%)
      val woman_number = all(7).replaceAll("%", "").toDouble + ""

      //员工平均年龄
      val work_avg_age = all(8) + ""

      //死亡案件数
      val death_number = all(9) + ""
      //伤残案件数
      val disable_number = all(10) + ""
      //报案总数
      val number = all(13) + ""

      //平均出险周期
      val avg_risk_cycle = all(14) + ""

      //企业的潜在客户
      val potential_people = all(17) + ""
      //城市名称,并得到城市编码
      val city_name = all(18).replaceAll("市", "") + ""
      val city_node = d_city_grade_map.getOrElse(city_name, "0")

      //实际赔付
      val actual_payment = all(19) + ""
      //预估赔付
      val estimated_payment = all(20) + ""
      //已赚保费
      val transfer_premium = all(21) + ""


      //风险等级
      val risk_level = all(24) + ""
      (
        work_type, protect_number, man_number, woman_number, work_avg_age, death_number,
        disable_number, number, avg_risk_cycle, potential_people, city_node, actual_payment,
        estimated_payment, transfer_premium, risk_level
      )
    })
      .toDF(
        "work_type_number", "protect_number", "man_number", "woman_number", "work_avg_age", "death_number",
        "disable_number", "number", "avg_risk_cycle", "potential_people", "city_node", "actual_payment",
        "estimated_payment", "transfer_premium", "risk_level"
      )
      .insertInto("enter_model_data", overwrite = true)

    sc.stop()
  }
}
