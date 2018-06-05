package company.sparkMLlib.history_date_model

import java.util.Properties

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
  * Created by MK on 2018/5/24.
  */
object person_and_enter {


  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf_spark = new SparkConf().setAppName("wuYu")
    conf_spark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf_spark.registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
    conf_spark.set("spark.sql.broadcastTimeout", "36000")
      .setMaster("local[2]")

    val sc = new SparkContext(conf_spark)

    val sqlContext: HiveContext = new HiveContext(sc)


    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "172.16.11.106")

    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "labels:label_user_enterprise_vT")

    val usersRDD = sc.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    )
    import sqlContext.implicits._

    val result = usersRDD.map { x => {
      val s: (ImmutableBytesWritable, Result) = x
      val ent_man_woman_proportion = Bytes.toString(s._2.getValue("baseinfo".getBytes, "ent_man_woman_proportion".getBytes))
      val ent_scale = Bytes.toString(s._2.getValue("baseinfo".getBytes, "ent_scale".getBytes))
      val str = if (ent_man_woman_proportion == null) null else ent_man_woman_proportion.replaceAll(",", "-")
      //x|男女比例 | 总人数
      //      (x, str, str_ent_scale)
      (x, str, ent_scale)
    }
    }.map(x => {
      val ent_name = Bytes.toString(x._1._2.getValue("baseinfo".getBytes, "ent_name".getBytes))
      //ent_id //rowKey
      val kv = x._1._2.getRow
      val rowkey = Bytes.toString(kv)
      (rowkey, ent_name)
    }).toDF("ent_id", "ent_name")


    val url = "jdbc:mysql://172.16.11.103:3306/odsdb?user=root&password=123456"

    val prop = new Properties()
    val ent_risk_value = sqlContext.read.jdbc(url, "ent_risk_value", prop).select("ent_id", "EntRiskLevel", "EntValueScore")


    val tepOne = sqlContext.sql("select * from personal_model_data")

    //加载个人的模型
    val tep_four = tepOne.map(x => {
      //得到特征维度
      val money = x.getAs("money").toString
      val early_res = x.getAs("early_res").toString
      val user_craft_level = x.getAs("user_craft_level").toString
      val age = x.getAs("age").toString
      val city_level = x.getAs("city_level").toString
      val sex = x.getAs("sex").toString
      val Insure_to_month = x.getAs("insure_to_month").toString
      val res = Array(money, early_res, user_craft_level, age, city_level, sex, Insure_to_month).map(_.toDouble)

      val ent_id = x.getAs("ent_id").toString


      val cert_no = x.getAs[String]("cert_no")
      (cert_no, ent_id, Vectors.dense(res))
    }).toDF("cert_no", "ent_id", "person_risk_vector")
    val scaler_one = new MinMaxScaler().setInputCol("person_risk_vector").setOutputCol("person_risk_vector_features")
    val scalerModel_one = scaler_one.fit(tep_four)
    val scaledData_one = scalerModel_one.transform(tep_four)

    //加载个人风险的模型
    val person_risk_model = KMeansModel.load(sc, "hdfs://namenode1.cdh:8020/model/person_risk")
//个人
    val tep_three = scaledData_one.map(x => {
      val one = x.getAs[org.apache.spark.mllib.linalg.Vector]("person_risk_vector_features")
      val cert_no = x.getAs[String]("cert_no")
      val person_risk_vector = x.getAs("person_risk_vector").toString
      val ent_id = x.getAs("ent_id").toString
      (cert_no, person_risk_model.predict(one), person_risk_vector, ent_id)
    }).toDF("cert_no", "person_risk", "person_value", "ent_id").select("cert_no","person_risk","person_value").take(20).foreach(println(_))

//企业
//    val tep_one = sqlContext.sql("select * from model_final_value").join(ent_risk_value, "ent_id")
//      .map(x => {
//        val ent_id = x.getAs("ent_id").toString
//        val enter_value = x.getAs("enter_value").toString
//        val risk_value = x.getAs("risk_value").toString
//        val value_date = x.getAs("value_date").toString.split("\t").mkString("/")
//        val risk_date = x.getAs("risk_date").toString
//        val EntRiskLevel = x.getAs("EntRiskLevel").toString
//        val EntValueScore = x.getAs("EntValueScore").toString
//        (ent_id, enter_value, risk_value, value_date, risk_date, EntRiskLevel, EntValueScore)
//      }).toDF("ent_id", "enter_value", "risk_value", "value_date", "risk_date", "EntRiskLevel", "EntValueScore")
//      .join(result, "ent_id")
//
//    tep_three.repartition(1).write.format("com.databricks.spark.csv")
//      .option("header", "true") //在csv第一行有属性"true"，没有就是"false"
//      .option("delimiter", ",") //默认以","分割
//      .save("C:\\Users\\a2589\\Desktop\\person")

    //分为了7类 0,1,2,3,4,5,6
    /**
      * 分类/风险等级
      * 1/0
      * 2/2
      * 5/3
      * 6/4
      * 4/5
      * 0/6
      * 3/6
      * */
  }
}
