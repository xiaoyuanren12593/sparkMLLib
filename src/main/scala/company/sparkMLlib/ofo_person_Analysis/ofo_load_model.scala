package company.sparkMLlib.ofo_person_Analysis

import com.alibaba.fastjson.JSON
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MK on 2018/6/29.
  */
object ofo_load_model {


  val conf_spark = new SparkConf().setAppName("wuYu")
  conf_spark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf_spark.registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
  conf_spark.set("spark.sql.broadcastTimeout", "36000")
  //    .setMaster("local[2]")

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

    /** 得到省份
      * d_cant	省市区码表	省市区信息
      * */
    val d_cant = sqlContext.sql("select * from odsdb_prd.d_cant").filter("length(name)>0").cache

    //通过省区的名字，找出对应的code
    val cert_native_province_r = d_cant.map(x => {
      val code = x.getString(0)
      val name = x.getString(1)
      (name, code)
    }).filter(_._2.contains("0000")).collectAsMap


    //加载模型
    //    val ofo_model = KMeansModel.load(sc, "hdfs://namenode1.cdh:8020/model/only_ofo")
    val ofo_model = KMeansModel.load(sc, "/model/only_ofo")
    import sqlContext.implicits._
    val result = usersRDD.map { x => {
      val s: (ImmutableBytesWritable, Result) = x
      val ent_man_woman_proportion = Bytes.toString(s._2.getValue("goout".getBytes, "user_person_only_ofo".getBytes))
      (x._2, ent_man_woman_proportion)
    }
    }.filter(_._2 != null).map(x => {
      val json_arr = JSON.parseObject(x._2)
      val all = json_arr.getString("user_information").split(",")
      //得到身份证号
      val id_card = Bytes.toString(x._1.getRow)
      //得到年龄
      val age = if (all(1).length > 0) all(1) else "null"
      val get_Province = if (all(3).length > 0) all(3) else "null"
      //得到对应省份的代码编号
      val province_code = cert_native_province_r.getOrElse(get_Province, "0.00")
      //投保次数
      val user_insure_product_num = json_arr.get("user_insure_product_num")

      //早起族还是夜猫子
      val isnot_early_night = json_arr.get("early_night")
      val early_night_end = if (isnot_early_night == null) 0.0 else 1.0

      (id_card, age, province_code, user_insure_product_num, early_night_end, get_Province)
    }).filter(x => x._2 != "null" && x._3 != "0.00" && x._4 != null).map(x => {
      val insure_no = x._1
      val age = x._2
      val province_code = x._3
      val user_insure_product_num = x._4
      val early_night_end = x._5
      val get_Province_name = x._6
      val end_value = Array(age, province_code, user_insure_product_num, early_night_end).map(_.toString.toDouble)
      (insure_no, Vectors.dense(end_value), get_Province_name)
    }).toDF("label", "features", "province")


    val scaler_M = new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures")
    val scalerModel = scaler_M.fit(result)
    val scaledData = scalerModel.transform(result)
    val ofo_k_means = scaledData.map(x => {
      val scaledFeatures = x.getAs[org.apache.spark.mllib.linalg.Vector]("scaledFeatures")
      //身份证
      val insure_no = x.getAs[String]("label")
      //年龄-省份编码-投保次数-早起晚起是与否
      val features = x.getAs[org.apache.spark.mllib.linalg.Vector]("features").toString
      //省份名称
      val province_name = x.getAs[String]("province")
      //预测结果
      val ofo_predict_result = ofo_model.predict(scaledFeatures)
      (insure_no, features, province_name, ofo_predict_result)
    }).toDF("insure_no", "features", "province_name", "ofo_predict_result")
    ofo_k_means.insertInto("ofo_model_data", overwrite = true)

  }
}

