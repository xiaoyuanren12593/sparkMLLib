package bzn.job.mllib

import bzn.job.common.Until
import com.alibaba.fastjson.JSON
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MK on 2018/4/18.
  * 已在oozie中每天更新表
  */
object read_hbaseTo_hive extends Until {
  def bolZero(str: String): String = if (str == null) "0" else str

  //读取个人标签的数据
  def read_hbase_person(usersRDD: RDD[(ImmutableBytesWritable, Result)]): RDD[(String, String, String, String, String, String, String, String, String)] = {
    val res_k_means: RDD[(String, String, String, String, String, String, String, String, String)] = usersRDD
      .mapPartitions(rdd => {
        rdd.map(x => {
          val s: (ImmutableBytesWritable, Result) = x
          //个人对应的企业ID
          val user_ent_id_before = Bytes.toString(s._2.getValue("baseinfo".getBytes, "user_ent_id".getBytes))
          val user_ent_id = if (user_ent_id_before != null) user_ent_id_before else "null"

          //个人工种等级
          val user_craft_level = bolZero(Bytes.toString(s._2.getValue("baseinfo".getBytes, "user_craft_level".getBytes)))

          //实际已赔付金额
          val finalpay_total = bolZero(Bytes.toString(s._2.getValue("baseinfo".getBytes, "finalpay_total".getBytes)))

          //预估总赔付金额
          val prepay_total = bolZero(Bytes.toString(s._2.getValue("baseinfo".getBytes, "prepay_total".getBytes)))

          //赔付金额
          val money = if (finalpay_total == "0") prepay_total else finalpay_total

          //年龄,城市位于几线
          val age_tepOne = Bytes.toString(s._2.getValue("baseinfo".getBytes, "user_person_none_ofo".getBytes))
          val age_city_tepTwo = if (age_tepOne != null) {
            val tepThree = JSON.parseObject(age_tepOne).getJSONObject("personal_Information").getString("user_information").split(",")
            val tep_four = if (tepThree(6) == "一线城市") "1" else if (tepThree(6) == "二线城市") "2" else if (tepThree(6) == "三线城市") "3" else if (tepThree(6) == "四线城市") "4" else if (tepThree(6) == "五线城市") "5" else "0"
            (tepThree(1), tep_four)
          } else ("0", "0")

          val none_ofo = Bytes.toString(s._2.getValue("baseinfo".getBytes, "user_person_none_ofo".getBytes))
          val custom = if (none_ofo != null) JSON.parseObject(none_ofo).getJSONObject("fist_now_month_and_product").getString("customer_Source") else "null"

          //首次投保至今月份
          val Insure_to_month = if (none_ofo != null) JSON.parseObject(none_ofo).getJSONObject("fist_now_month_and_product").getString("Insure_to_month") else "0"

          val only_ofo = Bytes.toString(s._2.getValue("goout".getBytes, "user_person_only_ofo".getBytes))
          val early_night = if (only_ofo != null) JSON.parseObject(only_ofo).getString("early_night") else "null"
          //如果 early_night等于早起族 用数字1表示
          //如果 early_night等于夜猫子 用数字2表示
          //如果 early_night等于null 用数字0表示
          val early_res = if (early_night == "早起族") "1" else if (early_night == "夜猫子") "2" else "0"

          //rowKey
          val kv = x._2.getRow
          val rowkey = Bytes.toString(kv)
          //性别
          val sex = if (none_ofo != null) JSON.parseObject(none_ofo).getJSONObject("personal_Information").getString("user_information") else "3"
          val end_woman_man = if (sex.contains("女")) "0" else if (sex.contains("男")) "1" else "3"

          //37022319620505152X,126,finalpay_total
          //      产品来源 ，赔付金额 ，是否是早起族或者是夜猫子，个人工种等级，年龄，城市位于几线,男或者女,首次投保至今的月份,rowkey,企业ID
          val ss = (custom, money, early_res, user_craft_level, age_city_tepTwo._1, age_city_tepTwo._2, end_woman_man, Insure_to_month, rowkey, user_ent_id)

          val arr_res = (ss._2, ss._3, ss._4, ss._5, ss._6, ss._7, ss._8, ss._9, ss._10)
          (ss._1, arr_res)
        })
      })
      //只把官网和相关平台的拿出来
      .filter(x => if (x._1 != "null") true else false)
      .map(x => x._2)

    res_k_means
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf_spark = new SparkConf().setAppName("wuYu").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setMaster("local[2]")
    val sc = new SparkContext(conf_spark)

    val sqlContext = new HiveContext(sc)
    /**
      * 第一步:创建一个JobConf
      **/
    //定义HBase的配置
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "172.16.11.106")
    conf.set("mapreduce.task.timeout", "1200000")
    conf.set("hbase.client.scanner.timeout.period", "600000")
    conf.set("hbase.rpc.timeout", "600000")

    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "labels:label_user_personal_vT")
    val usersRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    )
    //    val count = usersRDD.count()
    //    println("copy_m RDD Count" + count)
    import sqlContext.implicits._

    //读取个人标签的数据
    val res_k_means = read_hbase_person(usersRDD)
      //赔付金额 ，是否是早起族或者是夜猫子，个人工种等级，年龄，城市位于几线,男或者女,首次投保至今的月份,rowkey
      .toDF("money", "early_res", "user_craft_level", "age", "city_level", "sex", "insure_to_month", "cert_no", "ent_id")

    //将个人标签存入到hbase中
    val conf_person = HbaseConf("labels:label_user_personal_vT")._1
    val conf_fs_person = HbaseConf("labels:label_user_personal_vT")._2
    val tableName_person = "labels:label_user_personal_vT"
    val columnFamily1_person = "baseinfo"

    val vectors = res_k_means.map(x => {
      val cert_no = x.getAs("cert_no").toString
      val money = x.getAs("money").toString
      val early_res = x.getAs("early_res").toString
      val user_craft_level = x.getAs("user_craft_level").toString
      val age = x.getAs("age").toString
      val city_level = x.getAs("city_level").toString
      val sex = x.getAs("sex").toString
      val insure_to_month = x.getAs("insure_to_month").toString

      (cert_no, Vectors.dense(Array(money, early_res, user_craft_level, age, city_level, sex, insure_to_month).map(_.toDouble)))
    }).toDF("cert_no", "features")

    //加载个人风险模型
    val person_model = KMeansModel.load(sc, "hdfs://namenode1.cdh:8020/model/person_risk")

    val scaler_M = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
    val scalerModel = scaler_M.fit(vectors)
    val scaledData = scalerModel.transform(vectors)

    val person_value_model = scaledData.map(x => {
      val cert_no = x.getAs("cert_no").toString
      val scaledFeatures = x.getAs[org.apache.spark.mllib.linalg.Vector]("scaledFeatures")
      val person_risk = person_model.predict(scaledFeatures)
      (cert_no, person_risk + "", "person_value_model")
    })

    toHbase(person_value_model, columnFamily1_person, "person_value_model", conf_fs_person, tableName_person, conf_person)


    //将其存入到hive中
    res_k_means.insertInto("personal_model_data", overwrite = true)

  }
}
