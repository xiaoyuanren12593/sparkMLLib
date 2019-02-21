package bzn.label.channel_insureinfo

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by MK on 2018/11/5.
  */
object ChaInsureinfo extends ChaInsureinfoUntil{

  def Insure(usersRDD: RDD[String],
             channel_ent_name: Array[String],
             ods_ent_guzhu_salesman_channel: RDD[(String, String)],
             sqlContext: HiveContext,
             get_hbase_key_name: collection.Map[String, String],
             en_before: collection.Map[String, String]
            )
  : Unit
  = {
    val ent_summary_month_1 = sqlContext.sql("select * from odsdb_prd.ent_summary_month_1").cache()

    val ods_policy_detail: DataFrame = sqlContext.sql("select * from odsdb_prd.ods_policy_detail").cache()
    //被保人明细表
    val ods_policy_insured_detail = sqlContext.sql("select * from odsdb_prd.ods_policy_insured_detail")
    val ent_sum_level = sqlContext.sql("select * from odsdb_prd.ent_sum_level").cache()

    //HBaseConf
    val conf: Configuration = HbaseConf("labels:label_channel_vT")._1
    val conf_fs: Configuration = HbaseConf("labels:label_channel_vT")._2
    val tableName: String = "labels:label_channel_vT"
    val columnFamily1: String = "insureinfo"

    //过滤处字符串中存在上述渠道企业的数据
    val before: RDD[(String, String)] = usersRDD.map(x => (x.split("mk6")(0), x.split("mk6")(1)))

    //过滤出标签中的渠道
    //    val channel_before: RDD[(String, String)] = before.filter(x => channel_ent_name.contains(x._1))

    //    //得到渠道名称和对应的rowkey(老板)
    //    val en = channel_before.map(x => x._2.split(";").filter(_.contains("ent_name")).take(1).mkString("")).filter(_.length > 1).map(end => {
    //      val before = JSON.parseObject(end)
    //      (s"${before.getString("row")}_channel", before.getString("value"), before.getString("qual"))
    //    }).filter(_._1.length > 5)
    //    val en_before: collection.Map[String, String] = en.map(x => (x._2, x._1)).collectAsMap

    //渠道累计增减员次数
    val ent_add_regulation_times_data = channel_add(before, ods_ent_guzhu_salesman_channel, sqlContext, en_before, "ent_add_regulation_times").filter(_._1.length > 5)
    toHbase(ent_add_regulation_times_data, columnFamily1, "ent_add_regulation_times", conf_fs, tableName, conf)

    //渠道月均增减员次数
    val ent_month_regulation_times_r_before = before.map(x => (x._1, x._2.split(";").filter(_.contains("ent_add_regulation_times")).take(1).mkString(""))).filter(_._2.length > 1).map(end => {
      val before = JSON.parseObject(end._2)
      (before.getString("row"), before.getString("value"), before.getString("qual"))
    }).cache
    val ent_month_regulation_times_r = ent_month_regulation_times(ent_month_regulation_times_r_before, ent_summary_month_1, get_hbase_key_name, sqlContext, ods_ent_guzhu_salesman_channel, en_before).filter(_._1.length > 5)
    toHbase(ent_month_regulation_times_r, columnFamily1, "ent_month_regulation_times", conf_fs, tableName, conf)


    //渠道累计增员人数
    val ent_add_sum_persons_r = channel_add(before, ods_ent_guzhu_salesman_channel, sqlContext, en_before, "ent_add_sum_persons").filter(_._1.length > 5)
    toHbase(ent_add_sum_persons_r, columnFamily1, "ent_add_sum_persons", conf_fs, tableName, conf)

    //渠道累计减员人数
    val ent_del_sum_persons_r = channel_add(before, ods_ent_guzhu_salesman_channel, sqlContext, en_before, "ent_del_sum_persons").filter(_._1.length > 5)
    toHbase(ent_del_sum_persons_r, columnFamily1, "ent_del_sum_persons", conf_fs, tableName, conf)


    //渠道月均在保人数
    val ent_month_plc_persons_r = ent_month_plc_persons(ent_summary_month_1, get_hbase_key_name, sqlContext, ods_ent_guzhu_salesman_channel, en_before).filter(_._1.length > 5)
    toHbase(ent_month_plc_persons_r, columnFamily1, "ent_month_plc_persons", conf_fs, tableName, conf)


    //渠道续投人数
    val ent_continuous_plc_persons_r = channel_add(before, ods_ent_guzhu_salesman_channel, sqlContext, en_before, "ent_continuous_plc_persons").filter(_._1.length > 5)
    toHbase(ent_continuous_plc_persons_r, columnFamily1, "ent_continuous_plc_persons", conf_fs, tableName, conf)


    //渠道投保工种数
    val ent_insure_craft_r = channel_add(before, ods_ent_guzhu_salesman_channel, sqlContext, en_before, "ent_insure_craft").filter(_._1.length > 5)
    toHbase(ent_insure_craft_r, columnFamily1, "ent_insure_craft", conf_fs, tableName, conf)


    //求出该渠道中第一工种出现的类型哪个最多
    val ent_first_craft_r = channel_add_type(before, ods_ent_guzhu_salesman_channel, sqlContext, en_before, "ent_first_craft").filter(_._1.length > 5)
    toHbase(ent_first_craft_r, columnFamily1, "ent_first_craft", conf_fs, tableName, conf)


    //求出该渠道中第二工种出现的类型哪个最多
    val ent_second_craft_r = channel_add_type(before, ods_ent_guzhu_salesman_channel, sqlContext, en_before, "ent_second_craft").filter(_._1.length > 5)
    toHbase(ent_second_craft_r, columnFamily1, "ent_second_craft", conf_fs, tableName, conf)

    //求出该渠道中第三工种出现的类型哪个最多
    val ent_third_craft_r = channel_add_type(before, ods_ent_guzhu_salesman_channel, sqlContext, en_before, "ent_third_craft").filter(_._1.length > 5)
    toHbase(ent_third_craft_r, columnFamily1, "ent_third_craft", conf_fs, tableName, conf)


    //该渠道中哪个工种类型的赔额额度最高
    val ent_most_money_craft_r = channel_add_type(before, ods_ent_guzhu_salesman_channel, sqlContext, en_before, "ent_most_money_craft").filter(_._1.length > 5)
    toHbase(ent_most_money_craft_r, columnFamily1, "ent_most_money_craft", conf_fs, tableName, conf)

    //该渠道中哪个工种类型出险最多
    val ent_most_count_craft_r = channel_add_type(before, ods_ent_guzhu_salesman_channel, sqlContext, en_before, "ent_most_count_craft").filter(_._1.length > 5)
    toHbase(ent_most_count_craft_r, columnFamily1, "ent_most_count_craft", conf_fs, tableName, conf)


    //渠道投保人员占总人数比
    val insured_rate_r = insured_rate(ods_policy_detail, ods_policy_insured_detail, ent_sum_level, get_hbase_key_name, sqlContext, ods_ent_guzhu_salesman_channel, en_before).filter(_._1.length > 5)
    toHbase(insured_rate_r, columnFamily1, "insured_rate", conf_fs, tableName, conf)


    //渠道有效保单数
    val effective_policy_r = channel_add(before, ods_ent_guzhu_salesman_channel, sqlContext, en_before, "effective_policy").filter(_._1.length > 5)
    toHbase(effective_policy_r, columnFamily1, "effective_policy", conf_fs, tableName, conf)

    //渠道累计投保人次(不对身份证去重)
    val total_insured_count_r = channel_add(before, ods_ent_guzhu_salesman_channel, sqlContext, en_before, "total_insured_count").filter(_._1.length > 5)
    toHbase(total_insured_count_r, columnFamily1, "total_insured_count", conf_fs, tableName, conf)

    //渠道累计投保人数 totalInsuredPersons（去重）对身份证号去重
    val total_insured_persons_r = channel_add(before, ods_ent_guzhu_salesman_channel, sqlContext, en_before, "total_insured_persons").filter(_._1.length > 5)
    toHbase(total_insured_persons_r, columnFamily1, "total_insured_persons", conf_fs, tableName, conf)

    //当前渠道在保人数
    val cur_insured_persons_r = channel_add(before, ods_ent_guzhu_salesman_channel, sqlContext, en_before, "cur_insured_persons").filter(_._1.length > 5)
    toHbase(cur_insured_persons_r, columnFamily1, "cur_insured_persons", conf_fs, tableName, conf)

    //渠道累计保费
    val total_premium_data = channel_add(before, ods_ent_guzhu_salesman_channel, sqlContext, en_before, "total_premium").filter(_._1.length > 5)
    toHbase(total_premium_data, columnFamily1, "total_premium", conf_fs, tableName, conf)


    //渠道月均保费
    val avg_month_premium_r = avg_month_premium(ent_summary_month_1, get_hbase_key_name, sqlContext, ods_ent_guzhu_salesman_channel, en_before, before, "avg_month_premium").filter(_._1.length > 5)
    toHbase(avg_month_premium_r, columnFamily1, "avg_month_premium", conf_fs, tableName, conf)

    //渠道连续在保月份，都有哪个月
    val month_number = channel_add_month(before, ods_ent_guzhu_salesman_channel, sqlContext, en_before, "month_number").filter(_._1.length > 5)
    toHbase(month_number, columnFamily1, "month_number", conf_fs, tableName, conf)

    //渠道连续在保月数
    //月份的增加和减少
    def month_add_jian(number: Int, filter_date: String): String = {
      //当前月份+1
      val sdf = new SimpleDateFormat("yyyyMM")
      val dt = sdf.parse(filter_date)
      val rightNow = Calendar.getInstance()
      rightNow.setTime(dt)
      rightNow.add(Calendar.MONTH, number)
      val dt1 = rightNow.getTime()
      val reStr = sdf.format(dt1)
      reStr
    }

    val ent_continuous_plc_month_r = channel_add_month(before, ods_ent_guzhu_salesman_channel, sqlContext, en_before, "month_number").filter(_._1.length > 5).map(x => (x._1, x._2, "ent_continuous_plc_month")).map(x => {
      val tep_three = if (!x._2.contains("-") && x._2.length > 0) 1 else if (x._2.contains("-")) {
        //最近的一次断开的月份，得到连续在保月份
        val res = x._2.split("-").sorted
        val first_data = res(0)
        val final_data = res(res.length - 1)
        //2个日期相隔多少个月，包括开始日期和结束日期
        val get_res_day = getBeg_End_one_two_month(first_data, final_data)

        //找出最近的一次的连续日期
        val filter_date = if (res.length == get_res_day.length) month_add_jian(0, res(0))
        else {
          val res_end = get_res_day.filter(!res.contains(_)).reverse(0)
          month_add_jian(1, res_end)
        }
        //得到2个日期之间相隔多少个月
        val end_final: Int = getBeg_End_one_two_month(filter_date, final_data).length
        end_final
      } else 0
      (x._1, tep_three.toString, x._3)
    }).filter(_._1.length > 5)
    toHbase(ent_continuous_plc_month_r, columnFamily1, "ent_continuous_plc_month", conf_fs, tableName, conf)


    //渠道首次投保至今月数
    val ent_fist_plc_month_r = ent_fist_plc_month(before, ods_ent_guzhu_salesman_channel, sqlContext, en_before, "ent_fist_plc_month").filter(_._1.length > 5)
    toHbase(ent_fist_plc_month_r, columnFamily1, "ent_fist_plc_month", conf_fs, tableName, conf)


    //渠道人均保费:渠道累计保费/渠道累计投保人数
    val before_premium: RDD[(String, String)] = total_premium_data.map(x => (x._1, x._2)).cache
    val after_premium: RDD[(String, String)] = total_insured_persons_r.map(x => (x._1, x._2))
    val avg_person_premium: RDD[(String, String, String)] = before_premium.join(after_premium).map(x => {
      val avg_premium = x._2._1.toDouble / x._2._2.toDouble
      (x._1, avg_premium.toString, "avg_person_premium")
    }).filter(_._1.length > 5)
    toHbase(avg_person_premium, columnFamily1, "avg_person_premium", conf_fs, tableName, conf)


    //渠道年均保费(渠道月均保费*12)
    val avg_year_premium = avg_month_premium_r.map(x => {
      val year_premium = x._2.toDouble * 12
      (x._1, year_premium.toString, "avg_year_premium")
    })
    toHbase(avg_year_premium, columnFamily1, "avg_year_premium", conf_fs, tableName, conf)


    //渠道当前生效保单数
    val cureffected_policy = channel_add(before, ods_ent_guzhu_salesman_channel, sqlContext, en_before, "cureffected_policy").filter(_._1.length > 5)
    toHbase(cureffected_policy, columnFamily1, "cureffected_policy", conf_fs, tableName, conf)

  }

  def main(args: Array[String]): Unit = {
    //得到标签数据

    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf_spark = new SparkConf().setAppName("Cha_insureinfo")
    conf_spark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf_spark.registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
    conf_spark.set("spark.sql.broadcastTimeout", "36000")
    //      .setMaster("local[4]")

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
    val ods_ent_guzhu_salesman_channel_only_channel = ods_ent_guzhu_salesman_channel.groupByKey.map(x => (x._1, x._2.mkString("mk6"))).persist(StorageLevel.MEMORY_ONLY)

    //得到渠道企业
    val channel_ent_name: Array[String] = ods_ent_guzhu_salesman_channel_only_channel.map(_._1).collect

    //得到标签数据
    val usersRDD: RDD[String] = getHbase_value(sc).map(tuple => tuple._2).map(result => {
      val ent_name = Bytes.toString(result.getValue("baseinfo".getBytes, "ent_name".getBytes))
      (ent_name, result.raw)
    }).mapPartitions(rdd => {
      val json: JSONObject = new JSONObject
      rdd.map(f => KeyValueToString(f._2, json, f._1)) //.flatMap(_.split(";"))
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
