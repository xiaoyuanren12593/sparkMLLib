package bzn.job.label.channel_baseinfo

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by MK on 2018/10/31.
  */
object ChaBaseinfoTest extends ChaBaseinfoUntilTest {

  /**
    * 渠道的投保年龄
    *
    * @param ods_policy_insured_detail      保全总表
    * @param ods_policy_detail_table        企业保单表
    * @param get_hbase_key_name             得到标签的key与企业名称Map集合
    * @param sqlContext                     sqlContext
    * @param ods_ent_guzhu_salesman_channel 渠道和企业名称表
    * @param en_before                      渠道和企业ID-Map集合
    * @return 渠道ID，投保年龄，ent_employee_age
    **/
  def qy_avg(ods_policy_insured_detail: DataFrame, ods_policy_detail_table: DataFrame,
             get_hbase_key_name: collection.Map[String, String], sqlContext: HiveContext,
             ods_ent_guzhu_salesman_channel: RDD[(String, String)],
             en_before: collection.Map[String, String]): RDD[(String, String, String)] = {
    import sqlContext.implicits._

    //计算该企业中:员工平均投保年龄(每个用户的投保日期-出生日期)求和后/企业的人数
    val ods_policy_insured_detail_table_age = ods_policy_insured_detail
      .filter("LENGTH(insured_start_date) > 1")
      .select("policy_id", "insured_birthday", "insured_start_date")
    val join_qy_mx_age = ods_policy_detail_table.join(ods_policy_insured_detail_table_age, "policy_id")
    val end = join_qy_mx_age
      .map(x => (x.getString(1), (x.getString(0), x.getString(2), x.getString(3))))
      .groupByKey
      .map(x => {
        val result = x._2
          .map(s => {
            (s._2, s._3)
          })
          .filter(x => if (x._1.contains("-") && x._2.contains("-")) true else false)
          .filter(f => if (f._1.split("-")(0).toInt > 1900) true else false)
          .map(m => {
            val mp = m._2.split(" ")(0)
            val mp_one = m._1.split(" ")(0)
            (mp_one, m._2, year_tb_one(mp_one, mp).toInt)
          })
        val q_length = result.size.toDouble
        val sum_date = result.map(_._3).sum.toDouble
        (s"${get_hbase_key_name.getOrElse(x._1, "null")}", sum_date.toString, q_length.toString)
      })
      .toDF("ent_name", "sum_date", "q_length")

    val ent_employee_age_end: RDD[(String, String, String)] = end
      .join(ods_ent_guzhu_salesman_channel.toDF("channel_name", "ent_name"), "ent_name")
      .map(x => {
        val channel_name = x.getAs[String]("channel_name")
        val sum_date = x.getAs[String]("sum_date").toFloat
        val q_length = x.getAs[String]("q_length").toFloat
        (channel_name, (sum_date, q_length))
      })
      .reduceByKey((x1, x2) => {
        val sum_date = x1._1 + x2._1
        val q_length = x1._2 + x2._2
        (sum_date, q_length)
      })
      .map(x => {
        val channel_name_id = en_before.getOrElse(x._1, "null")
        val sum_date = x._2._1
        val q_length = x._2._2
        val sq = sum_date / q_length
        (channel_name_id, sq.toString, "ent_employee_age")
      })
    ent_employee_age_end
  }

  def BaseInfo(users_RDD: RDD[String], channel_ent_name: Array[String],
               ods_ent_guzhu_salesman_channel_RDD: RDD[(String, String)],
               sql_context: HiveContext, get_hbase_key_name: collection.Map[String, String],
               en_before: collection.Map[String, String],ods_ent_guzhu_salesman_temp: RDD[(String, String)]): Unit = {

    val ods_policy_insured_detail = sql_context.sql("select * from odsdb_prd.ods_policy_insured_detail")

    val ods_policy_detail_table: DataFrame = sql_context.sql("select policy_id,ent_id from odsdb_prd.ods_policy_detail").cache()

    //HBaseConf
    val conf = HbaseConf("labels:label_channel_vT")._1
    val conf_fs = HbaseConf("labels:label_channel_vT")._2
    val tableName = "labels:label_channel_vT"
    val columnFamily = "baseinfo"

    //过滤处字符串中存在上述渠道企业的数据
    val before: RDD[(String, String)] = users_RDD.map(x => (x.split("mk6")(0), x.split("mk6")(1)))

    //过滤出标签中的渠道
    val channel_before = before.filter(x => channel_ent_name.contains(x._1))
    ods_ent_guzhu_salesman_temp.leftOuterJoin(channel_before)
    channel_before.take(10).foreach(println)
    channel_before.filter(x => x._1 == "重庆翔耀保险咨询服务有限公司").take(10).foreach(println)
    //渠道类型
    val et = channel_before
      .map(x => (x._1, x._2.split(";").filter(_.contains("ent_type")).take(1).mkString("")))
      .filter(_._2.length > 1)
      .map(end => {
        val before = JSON.parseObject(end._2)
        (s"${en_before.getOrElse(end._1, "null")}", before.getString("value"), before.getString("qual"))
      })
      .filter(_._1.length > 5)
    println(1)
    et.foreach(println)
//    saveToHbase(et, columnFamily, "ent_type", conf_fs, tableName, conf)

    //渠道的初次投保时间
    val rt = channel_before
      .map(x => (x._1, x._2.split(";").filter(_.contains("first_insure_time")).take(1).mkString("")))
      .filter(_._2.length > 1)
      .map(end => {
        val before = JSON.parseObject(end._2)
        (s"${en_before.getOrElse(end._1, "null")}", before.getString("value"), before.getString("qual"))
      })
      .filter(_._1.length > 5)
    println(2)
    rt.foreach(println)
//    saveToHbase(rt, columnFamily, "first_insure_time", conf_fs, tableName, conf)
//
    //渠道名称
    val en = channel_before
      .map(x => (x._1, x._2.split(";").filter(_.contains("ent_name")).take(1).mkString("")))
      .filter(_._2.length > 1)
      .map(end => {
        val before = JSON.parseObject(end._2)
        (s"${en_before.getOrElse(end._1, "null")}", before.getString("value"), before.getString("qual"))
      })
      .filter(_._1.length > 5)
        .filter(x => x._1 == "b5b8863be155c557c84d4addcd9b6b14")
    en.take(10).foreach(println)
//    saveToHbase(en, columnFamily, "ent_name", conf_fs, tableName, conf)

    //渠道名称ods_ent_guzhu_salesman_temp
//
//
    //渠道所在省份
    val ep = channel_before.map(x => (x._1, x._2.split(";").filter(_.contains("ent_province")).take(1).mkString("")))
      .filter(_._2.length > 1)
      .map(end => {
        val before = JSON.parseObject(end._2)
        (s"${en_before.getOrElse(end._1, "null")}", before.getString("value"), before.getString("qual"))
      })
      .filter(_._1.length > 5)
  .filter(x => x._1 == "b5b8863be155c557c84d4addcd9b6b14")
    ep.foreach(println)
////    saveToHbase(ep, columnFamily, "ent_province", conf_fs, tableName, conf)
//
//    //渠道所在城市
//    val ec = channel_before.map(x => (x._1, x._2.split(";").filter(_.contains("ent_city")).take(1).mkString("")))
//      .filter(_._2.length > 1)
//      .map(end => {
//        val before = JSON.parseObject(end._2)
//        (s"${en_before.getOrElse(end._1, "null")}", before.getString("value"), before.getString("qual"))
//      })
//      .filter(_._1.length > 5)
//    println(5)
//    ec.foreach(println)
////    saveToHbase(ec, columnFamily, "ent_city", conf_fs, tableName, conf)
//
//    //渠道所在的城市类型：ent_city_type
//    val ect = channel_before
//      .map(x => (x._1, x._2.split(";").filter(_.contains("ent_city_type")).take(1).mkString("")))
//      .filter(_._2.length > 1)
//      .map(end => {
//        val before = JSON.parseObject(end._2)
//        (s"${en_before.getOrElse(end._1, "null")}", before.getString("value"), before.getString("qual"))
//      })
//      .filter(_._1.length > 5)
//    println(6)
//    ect.foreach(println)
////    saveToHbase(ect, columnFamily, "ent_city_type", conf_fs, tableName, conf)
//
//    //渠道产品ID
//    val qCp = getqCp(en_before, before, users_RDD, channel_ent_name, ods_ent_guzhu_salesman_channel_RDD, sql_context)
//      .filter(_._1.length > 5)
//    println(7)
//    qCp.foreach(println)
////    saveToHbase(qCp, columnFamily, "ent_insure_code", conf_fs, tableName, conf)
//
//    //渠道的男女比例
//    val qy_sex_r = qy_sex(ods_policy_insured_detail, ods_policy_detail_table, get_hbase_key_name, sql_context,
//      ods_ent_guzhu_salesman_channel_RDD, en_before)
//      .filter(_._1.length > 5)
//    println(8)
//    qy_sex_r.foreach(println)
////    saveToHbase(qy_sex_r, columnFamily, "ent_man_woman_proportion", conf_fs, tableName, conf)
//
//    //渠道平均投保年龄
//    val qy_avg_r = qy_avg(ods_policy_insured_detail, ods_policy_detail_table, get_hbase_key_name, sql_context,
//      ods_ent_guzhu_salesman_channel_RDD, en_before)
//      .filter(_._1.length > 5)
//    println(9)
//    qy_avg_r.foreach(println)
////    saveToHbase(qy_avg_r, columnFamily, "ent_employee_age", conf_fs, tableName, conf)
//
//    //渠道的人员规模
//    val qy_gm_r = qy_gm(en_before, before, users_RDD, channel_ent_name, ods_ent_guzhu_salesman_channel_RDD, sql_context)
//      .filter(_._1.length > 5)
//    println(10)
//    qy_gm_r.foreach(println)
////    saveToHbase(qy_gm_r, columnFamily, "ent_scale", conf_fs, tableName, conf)
//
//    //渠道潜在人员规模
//    val qy_qz_r = qy_qz(en_before, before, users_RDD, channel_ent_name, ods_ent_guzhu_salesman_channel_RDD, sql_context)
//      .filter(_._1.length > 5)
//    println(11)
//    qy_qz_r.foreach(println)
////    saveToHbase(qy_qz_r, columnFamily, "ent_potential_scale", conf_fs, tableName, conf)
  }

  def main(args: Array[String]): Unit = {
    //得到标签数据
    val conf_spark = new SparkConf().setAppName("Cha_baseinfo")
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
    val ods_ent_guzhu_salesman_channel: RDD[(String, String)] = sqlContext.read.jdbc(location_mysql_url, "ods_ent_guzhu_salesman", prop)
      .map(x => {
        val ent_name = x.getAs[String]("ent_name").trim
        val channel_name = x.getAs[String]("channel_name").trim
        val new_channel_name = if (channel_name == "直客") ent_name else channel_name
        (new_channel_name, ent_name)
      })
    //以渠道分组后的渠道表
    val ods_ent_guzhu_salesman_channel_only_channel = ods_ent_guzhu_salesman_channel
      .groupByKey
      .map(x => (x._1, x._2.mkString("mk6")))
      .persist(StorageLevel.MEMORY_ONLY)

    //得到渠道企业
    val channel_ent_name: Array[String] = ods_ent_guzhu_salesman_channel_only_channel.map(_._1).collect

    //得到标签数据
    val usersRDD: RDD[String] = getHbaseValue(sc).map(tuple => tuple._2)
      .map(result => {
        val ent_name = Bytes.toString(result.getValue("baseinfo".getBytes, "ent_name".getBytes))
        (ent_name, result.raw)
      })
      .mapPartitions(rdd => {
        val json: JSONObject = new JSONObject
        rdd.map(f => KeyValueToString(f._2, json, f._1)) //.flatMap(_.split(";"))
      })
      .cache

    val get_hbase_key_name: collection.Map[String, String] = getHbaseValue(sc).map(tuple => tuple._2)
      .map(result => {
        val key = Bytes.toString(result.getRow)
        val ent_name = Bytes.toString(result.getValue("baseinfo".getBytes, "ent_name".getBytes))
        (key, ent_name)
      })
      .collectAsMap

    //渠道名称和渠道ID
    val en_before = sqlContext.read.jdbc(location_mysql_url, "ods_ent_guzhu_salesman", prop)
      .map(x => {
        val ent_name = x.getAs[String]("ent_name").trim
        val channel_name = x.getAs[String]("channel_name").trim
        val new_channel_name = if (channel_name == "直客") ent_name else channel_name
        val channel_id = x.getAs[String]("channel_id")
        (new_channel_name, channel_id)
      })
      .filter(x => if (x._1.length > 5 && x._2 != "null") true else false).collectAsMap

    val ods_ent_guzhu_salesman_temp: RDD[(String, String)] = sqlContext.read.jdbc(location_mysql_url, "ods_ent_guzhu_salesman", prop)
      .map(x => {
        val ent_name = x.getAs[String]("ent_name").trim
        val channel_name = x.getAs[String]("channel_name").trim
        val new_channel_name = if (channel_name == "直客") ent_name else channel_name
        val channel_id = x.getAs[String]("channel_id")
        (new_channel_name, channel_id)
      })

//      .filter(x => x._1 == "重庆翔耀保险咨询服务有限公司").foreach(println)
    //得到渠道名称和对应的rowkey
    //val en_before: collection.Map[String, String] = en.map(x => (x._2, x._1)).collectAsMap

    BaseInfo(usersRDD, channel_ent_name, ods_ent_guzhu_salesman_channel, sqlContext, get_hbase_key_name, en_before,ods_ent_guzhu_salesman_temp)
    sc.stop()
  }
}
