package bzn.job.label.channel_baseinfo

import java.text.NumberFormat
import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.ChronoField

import bzn.job.common.Until
import bzn.job.until.EnterpriseUntil
import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import sun.util.calendar.CalendarUtils.mod

/**
  * Created by MK on 2018/10/31.
  * baseinfo的实现方法
  */
trait ChaBaseinfoUntil extends Until  with EnterpriseUntil  {
  //得到投保人投保了多少年(麻烦)
  def year_tb_one(before: String, after: String): String = {
    val formatter_before = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val qs_hs: DateTimeFormatter = new DateTimeFormatterBuilder()
      .append(formatter_before)
      .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
      .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
      .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
      .toFormatter()

    val lod_before = LocalDateTime.parse(before, qs_hs)
    val lod_after = LocalDateTime.parse(after, qs_hs)
    //s1的年
    val s3_y = lod_before.getYear
    //s2的年
    val s4_y = lod_after.getYear

    var ss = s4_y - s3_y
    //s1的月
    val s5_m = lod_before.getMonth.getValue
    //s2的月
    val s6_m = lod_after.getMonth.getValue

    //s1的天
    val s7_d = lod_before.getDayOfMonth
    //s2的天
    val s8_d = lod_after.getDayOfMonth

    val result = if (s5_m < s6_m) {
      ss
    } else if (s5_m > s6_m) {
      ss - 1
    } else if (s5_m == s6_m) {
      val res = if (s7_d < s8_d) {
        ss
      } else if (s7_d > s8_d) {
        ss - 1
      } else if (s7_d == s8_d) {
        ss
      }
      res
    }
    s"$result"
  }

  /**
    * 渠道的男女比例
    *
    * @param ods_policy_insured_detail      保全总表
    * @param ods_policy_detail_table_T      企业保单表
    * @param get_hbase_key_name             得到标签的key与企业名称Map集合
    * @param sqlContext                     sqlContext
    * @param ods_ent_guzhu_salesman_channel 渠道和企业名称表
    * @param en_before                      渠道和企业ID-Map集合
    * @return 渠道ID，男女占比，ent_man_woman_proportion
    **/
  def qy_sex(ods_policy_insured_detail: DataFrame, ods_policy_detail_table_T: DataFrame,
             get_hbase_key_name: collection.Map[String, String], sqlContext: HiveContext,
             ods_ent_guzhu_salesman_channel: RDD[(String, String)],
             en_before: collection.Map[String, String]): RDD[(String, String, String)] = {
    import sqlContext.implicits._

    // 创建一个数值格式化对象(对数字)
    val numberFormat = NumberFormat.getInstance
    val ods_policy_insured_detail_table = ods_policy_insured_detail.filter("LENGTH(insured_cert_no)=18")
      .select("policy_id", "insured_name", "insured_cert_no")
    val join_qy_mx = ods_policy_detail_table_T.join(ods_policy_insured_detail_table, "policy_id")

    //该企业中男生，女生的数量
    val man_woman_percent_age_avg = join_qy_mx
      //|ent_id|policy_id|insured_name|insured_cert_no|
      .map(x => (x.getString(1), (x.getString(0), x.getString(2), x.getString(3))))
      .groupByKey
      .map(x => {
        // 计算该企业中女生和男生的占比
        val end_id: String = x._1
        //该企业有多少人
        val counts = x._2.map(_._3).size.toString
        //企业的男生和女生0或者1
        val people_number = x._2.map(s => {
          val str2 = s._3.substring(s._3.length - 2, s._3.length - 1)
          //这里截取的信息就是e，倒数第二个字符
          val ss = str2.toInt
          //取模求余:0是女生  1是男生
          val aa = mod(ss, 2)
          aa
        })
        //企业的男生和女生0或者1的数量
        val man_percentage = people_number.count(x => x == 1).toString
        val woman_percentage = people_number.count(x => x == 0).toString

        (s"${get_hbase_key_name.getOrElse(end_id, "null")}", man_percentage, woman_percentage, counts)
      }).toDF("ent_name", "man_Percentage", "woman_Percentage", "counts")

    //根据企业名称进行join
    val man_woman_percent_age_avg_end: RDD[(String, String, String)] = man_woman_percent_age_avg
      .join(ods_ent_guzhu_salesman_channel.toDF("channel_name", "ent_name"), "ent_name")
      .map(x => {
        val channel_name = x.getAs[String]("channel_name")
        val man_Percentage = x.getAs[String]("man_Percentage").toInt
        val woman_Percentage = x.getAs[String]("woman_Percentage").toInt
        val counts = x.getAs[String]("counts").toInt
        (channel_name, (man_Percentage, woman_Percentage, counts))
      })
      .reduceByKey((x1, x2) => {
        //根据渠道名称进行分组，并将各个企业的男生人数，女生人数，总人数求和
        val man_percentage = x1._1 + x2._1
        val woman_percentage = x1._2 + x2._2
        val counts = x1._3 + x2._3
        (man_percentage, woman_percentage, counts)
      })
      .map(x => {
        val channel_name_id = en_before.getOrElse(x._1, "null")
        val man_percentage = x._2._1
        val woman_percentage = x._2._2
        val counts = x._2._3
        numberFormat.setMaximumFractionDigits(2) //设置精确到小数点后2位
        //男生百分比/女生百分比
        val result_man: String = numberFormat.format(man_percentage.toFloat / counts.toFloat * 100)
        val result_woman: String = numberFormat.format(woman_percentage.toFloat / counts.toFloat * 100)
        (channel_name_id, s"$result_man%,$result_woman%", "ent_man_woman_proportion")
      })

    man_woman_percent_age_avg_end
  }

  //渠道产品ID
  def getqCp(en_before: collection.Map[String, String], before: RDD[(String, String)],
             usersRDD: RDD[String], channel_ent_name: Array[String],
             ods_ent_guzhu_salesman_channel: RDD[(String, String)],
             sqlContext: HiveContext): RDD[(String, String, String)] = {
    import sqlContext.implicits._

    //标签
    val ent_id = before
      .map(x => (x._1, x._2.split(";").filter(_.contains("ent_insure_code")).take(1).mkString("")))
      .filter(_._2.length > 1)
      .map(end => {
        val before = JSON.parseObject(end._2)
        //企业，和企业对应的数据
        (end._1, before.getString("value"), before.getString("qual"))
      })
      .toDF("ent_name", "value", "qual")
    val qCp: RDD[(String, String, String)] = ods_ent_guzhu_salesman_channel
      .toDF("channel_name", "ent_name")
      .join(ent_id, "ent_name")
      .map(x => (x.getAs[String]("channel_name"), x.getAs[String]("value")))
      .reduceByKey((x1, x2) => x1 + "|" + x2)
      .map(x => (en_before.getOrElse(x._1, "null"),
        if (x._2.contains("|")) x._2.split("\\|").distinct.mkString("|") else x._2, "ent_insure_code"))

    qCp
  }

  //渠道人员规模
  def qy_gm(en_before: collection.Map[String, String], before: RDD[(String, String)],
            usersRDD: RDD[String], channel_ent_name: Array[String], ods_ent_guzhu_salesman_channel: RDD[(String, String)],
            sqlContext: HiveContext): RDD[(String, String, String)] = {
    import sqlContext.implicits._

    //标签
    val ent_id = before
      .map(x => (x._1, x._2.split(";").filter(_.contains("ent_scale")).take(1).mkString("")))
      .filter(_._2.length > 1)
      .map(end => {
        val before = JSON.parseObject(end._2)
        //企业，和企业对应的数据
        (end._1, before.getString("value"), before.getString("qual"))
      })
      .toDF("ent_name", "value", "qual")

    val qy_gm_r: RDD[(String, String, String)] = ods_ent_guzhu_salesman_channel
      .toDF("channel_name", "ent_name")
      .join(ent_id, "ent_name")
      .map(x => (x.getAs[String]("channel_name"), x.getAs[String]("value").toInt))
      .reduceByKey((x1, x2) => x1 + x2)
      .map(x => (en_before.getOrElse(x._1, "null"), x._2.toInt.toString, "ent_scale"))

    qy_gm_r
  }

  //渠道潜在人员规模
  def qy_qz(en_before: collection.Map[String, String], before: RDD[(String, String)],
            usersRDD: RDD[String], channel_ent_name: Array[String], ods_ent_guzhu_salesman_channel: RDD[(String, String)],
            sqlContext: HiveContext): RDD[(String, String, String)] = {
    import sqlContext.implicits._

    //标签
    val ent_id = before
      .map(x => (x._1, x._2.split(";").filter(_.contains("ent_potential_scale")).take(1).mkString("")))
      .filter(_._2.length > 1)
      .map(end => {
        val before = JSON.parseObject(end._2)
        //企业，和企业对应的数据
        (end._1, before.getString("value"), before.getString("qual"))
      })
      .toDF("ent_name", "value", "qual")
    val qy_qz_r: RDD[(String, String, String)] = ods_ent_guzhu_salesman_channel
      .toDF("channel_name", "ent_name").join(ent_id, "ent_name")
      .map(x => (x.getAs[String]("channel_name"), x.getAs[String]("value").toInt))
      .reduceByKey((x1, x2) => x1 + x2)
      .map(x => (en_before.getOrElse(x._1, "null"), x._2.toInt.toString, "ent_potential_scale"))

    qy_qz_r
  }
}
