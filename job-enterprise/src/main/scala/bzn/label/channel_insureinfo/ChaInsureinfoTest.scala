package bzn.label.channel_insureinfo

import java.util.Properties

import com.alibaba.fastjson.JSONObject
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by MK on 2018/11/5.
  */
object ChaInsureinfoTest extends ChaInsureinfoUntil with until {

  def Insure(usersRDD: RDD[String],
             channel_ent_name: Array[String],
             ods_ent_guzhu_salesman_channel: RDD[(String, String)],
             sqlContext: HiveContext,
             get_hbase_key_name: collection.Map[String, String],
             en_before: collection.Map[String, String]
            )
  : Unit
  = {

    //过滤处字符串中存在上述渠道企业的数据
    val before: RDD[(String, String)] = usersRDD.map(x => (x.split("mk6")(0), x.split("mk6")(1)))

    //求出该渠道中第一工种出现的类型哪个最多
    val ent_second_craft_r =channel_add_type(before, ods_ent_guzhu_salesman_channel, sqlContext, en_before, "ent_first_craft").filter(_._1.length > 5)
    ent_second_craft_r.take(10).foreach(x => {
      println(s"数据1：${x._1} , 数据2：${x._2}, 数据3：${x._3}\n")
    })

    //求出该渠道中第二工种出现的类型哪个最多
    //    channel_add_type(before, ods_ent_guzhu_salesman_channel, sqlContext, en_before, "ent_second_craft").filter(_._1.length > 5)

    //求出该渠道中第三工种出现的类型哪个最多
    //    channel_add_type(before, ods_ent_guzhu_salesman_channel, sqlContext, en_before, "ent_third_craft").filter(_._1.length > 5)
  }

  def main(args: Array[String]): Unit = {
    //得到标签数据

    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf_spark = new SparkConf().setAppName("Cha_insureinfo")
    conf_spark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf_spark.registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
    conf_spark.set("spark.sql.broadcastTimeout", "36000")
      .setMaster("local[4]")

    val sc: SparkContext = new SparkContext(conf_spark)
    val sqlContext: HiveContext = new HiveContext(sc)


    //读取渠道表
    val lines_source = Source.fromURL(getClass.getResource("/config_scala.properties")).getLines.toSeq
    val location_mysql_url: String = lines_source(2).toString.split("==")(1)
    val prop: Properties = new Properties

    //读取渠道表（不分组）
    val ods_ent_guzhu_salesman_channel: RDD[(String, String)] = sqlContext.read.jdbc(location_mysql_url, "ods_ent_guzhu_salesman", prop).map(x => {
      val ent_name = x.getAs[String]("ent_name").trim
      val channel_name = x.getAs[String]("channel_name").trim
      val new_channel_name = if (channel_name == "直客") ent_name else channel_name
      (new_channel_name, ent_name)
    })

    //以渠道分组后的渠道表
    val ods_ent_guzhu_salesman_channel_only_channel = ods_ent_guzhu_salesman_channel.groupByKey
      .map(x => (x._1, x._2.mkString("mk6"))).persist(StorageLevel.MEMORY_ONLY)

    //得到渠道企业
    val channel_ent_name: Array[String] = ods_ent_guzhu_salesman_channel_only_channel.map(_._1).collect

    //得到标签数据
    val usersRDD: RDD[String] = getHbase_value(sc).map(tuple => tuple._2).map(result => {
      val ent_name = Bytes.toString(result.getValue("baseinfo".getBytes, "ent_name".getBytes))
      (ent_name, result.raw)
    }).mapPartitions(rdd => {
      val json: JSONObject = new JSONObject
      rdd.map(f => KeyValueToString(f._2, json, f._1))
    }).cache

    //得到标签数据企业ID，与企业名称
    val get_hbase_key_name: collection.Map[String, String] = getHbase_value(sc).map(tuple => tuple._2).map(result => {
      val key = Bytes.toString(result.getRow)
      val ent_name = Bytes.toString(result.getValue("baseinfo".getBytes, "ent_name".getBytes))
      (key, ent_name)
    }).collectAsMap

    //渠道名称和渠道ID
    val en_before: collection.Map[String, String] = sqlContext.read.jdbc(location_mysql_url, "ods_ent_guzhu_salesman", prop).map(x => {
      val ent_name = x.getAs[String]("ent_name").trim
      val channel_name = x.getAs[String]("channel_name").trim
      val new_channel_name = if (channel_name == "直客") ent_name else channel_name
      val channel_id = x.getAs[String]("channel_id")
      (new_channel_name, channel_id)
    }).filter(x => if (x._1.length > 5 && x._2 != "null") true else false).collectAsMap()


    Insure(usersRDD, channel_ent_name, ods_ent_guzhu_salesman_channel, sqlContext, get_hbase_key_name, en_before)

    sc.stop()
  }
}
