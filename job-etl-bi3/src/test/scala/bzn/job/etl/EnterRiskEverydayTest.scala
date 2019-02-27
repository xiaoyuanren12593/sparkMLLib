package company.canal_streaming

import bzn.job.common.Until
import bzn.job.until.BiUntil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MK on 2018/5/22.
  * 企业风险实时，每天运行，调用模型进行计算
  */
object EnterRiskEverydayTest extends Until with BiUntil {

  //企业风险::创建hbase配置文件
  def getHbase_conf(sc: SparkContext): RDD[(ImmutableBytesWritable, Result)]
  = {
    //定义HBase的配置
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "172.16.11.106")

    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "labels:label_user_enterprise_vT")

    val usersRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    )
    usersRDD
  }

  //企业价值:计算真实评分
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

  def main(args: Array[String]): Unit = {

    val conf_spark: SparkConf = new SparkConf().setAppName("wuYu")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
      .set("spark.sql.broadcastTimeout", "36000")
      .setMaster("local[2]")

    val sc: SparkContext = new SparkContext(conf_spark)
    val sqlContext: HiveContext = new HiveContext(sc)


    val dim_product = sqlContext.sql("select x.product_code from odsdb_prd.dim_product x where x.product_type_2='蓝领外包'").map(_.getAs("product_code").toString).collect()
    val bro = sc.broadcast(dim_product)
    val ods_policy_detail = sqlContext.sql("select ent_id,insure_code from odsdb_prd.ods_policy_detail").distinct()
    val d_city_grade_map: collection.Map[String, String] = city_code(sqlContext)
    val ods_bro = ods_policy_detail.map(x => {

      if (x.getAs("ent_id") != null) {
        val ent_id = x.getAs("ent_id").toString
        val insure_code = x.getAs("insure_code").toString
        (ent_id, insure_code)
      } else {
        ("", "")
      }

    }).filter(x => !x._1.equals("")).filter(x => bro.value.contains(x._2)).map(_._1).collect

    val ods_bro_end = sc.broadcast(ods_bro)

    /**
      * 从Hbase中读取数据
      **/
    val usersRDD = getHbase_conf(sc)

    import sqlContext.implicits._
    val tepOne: RDD[((ImmutableBytesWritable, Result), String, String)] = usersRDD.map { x => {
      val s: (ImmutableBytesWritable, Result) = x
      val ent_man_woman_proportion = Bytes.toString(s._2.getValue("baseinfo".getBytes, "ent_man_woman_proportion".getBytes))
      val ent_scale = Bytes.toString(s._2.getValue("baseinfo".getBytes, "ent_scale".getBytes))
      val str = if (ent_man_woman_proportion == null) null else ent_man_woman_proportion.replaceAll(",", "-")
      //x | 男女比例 | 总人数
      (x, str, ent_scale)
    }
    }
    //企业价值：得到标签数据
    val vectors = getHbase_label(tepOne, d_city_grade_map).filter(x => if (x._2.length > 0 && ods_bro_end.value.contains(x._2)) true else false).map(x => {
      (x._2, Vectors.dense(x._1), x._1(1))
    }).toDF("rowkey", "features", "online_protect")

    //企业价值：将特征进行标准化
    val scaler = new StandardScaler().setInputCol("features").setOutputCol("scaledFeatures").setWithStd(true).setWithMean(true)
    val scalerModel = scaler.fit(vectors)
    val train_data = scalerModel.transform(vectors)

    //企业风险：加载模型
    val modelLoad = DecisionTreeModel.load(sc, "hdfs://namenode1.cdh:8020/model/enter_risk")

    //通过rowkey与企业价值表进行关联，得到企业价值的特征维度
    val enterprise_premium_slope: DataFrame = sqlContext.sql("select * from odsdb_prd.enterprise_premium_slope").cache

    val enter_prise = train_data.join(enterprise_premium_slope, train_data("rowkey") === enterprise_premium_slope("ent_id"))
      .map(x => {
        val rowkey = x.getAs[String]("rowkey")
        //得到当前企业的在保人数，总保费，三月变化率进行评分
        val online_protect = x.getAs[Double]("online_protect")
        val premium_all = x.getAs[String]("premium_all").replaceAll(",", "").toDouble
        val three_slope = x.getAs[String]("three_slope").replaceAll("%", "").replaceAll(",", "").toDouble
        val end = get_Score(online_protect, premium_all, three_slope)

        //得到该企业的价值模型值:人数评分，保费评分，三月变化率评分 (求和),其用到的原始数据每天都会更新
        val enter_value = Array(end._1, end._2, end._3).map(_.toDouble).sum.formatted("%.2f")

        //得到该企业的风险模型值,通过模型的加载进行预测，其用到的原始数据是人工进行填写的(决策树)
        val data_predict = x.getAs[org.apache.spark.mllib.linalg.Vector]("scaledFeatures")
        val risk_value = modelLoad.predict(data_predict).toString

        val risk_date = x.getAs("features").toString

        (rowkey, enter_value, risk_value, s"$online_protect\t$premium_all\t$three_slope", risk_date)
      }).toDF("ent_id", "enter_value", "risk_value", "value_date", "risk_date")


    val conf = HbaseConf("labels:label_user_enterprise_vT")._1
    val conf_fs = HbaseConf("labels:label_user_enterprise_vT")._2
    val tableName = "labels:label_user_enterprise_vT"
    val columnFamily1 = "baseinfo"

    //将企业价值存到企业hbase中
    val one = enter_prise.map(x => {
      val ent_id = x.getAs("ent_id").toString
      val enter_value_model = x.getAs("enter_value").toString
      (ent_id, enter_value_model, "enter_value_model")
    })

    one.foreach(x => println(x))
    //    toHbase(one, columnFamily1, "enter_value_model", conf_fs, tableName, conf)

    //将企业风险等级存入hbase中
    val two = enter_prise.map(x => {
      val ent_id = x.getAs("ent_id").toString
      val risk_value_model = x.getAs("risk_value").toString
      (ent_id, risk_value_model, "risk_value_model")
    })

    two.foreach(x => println(x))
    //    toHbase(two, columnFamily1, "risk_value_model", conf_fs, tableName, conf)
    //
    //    //将其存入对应的hive表中
    //    enter_prise.insertInto("model_final_value", overwrite = true)

  }

}
