package bzn.job.transform.level

import java.io.File
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by MK on 2018/9/19.
  * 企业会员计算
  */

//读取企业每个月在保人数


object MembershipLevel {
  //得到2个日期之间的所有月份
  def getBeg_End_one_two_month(mon3: String, day_time: String): ArrayBuffer[String] = {
    val sdf = new SimpleDateFormat("yyyyMM")

    //得到今天的日期
    val cc = Calendar.getInstance
    cc.setTime(new Date)
    val day = cc.getTime

    //得到他们相间的所有日期
    val arr: ArrayBuffer[String] = ArrayBuffer[String]()
    val date_start = sdf.parse(mon3)
    //    val date_start = sdf.parse("20161007")
    val date_end = sdf.parse(day_time)
    var date = date_start
    //用Calendar 进行日期比较判断
    val cd = Calendar.getInstance

    while (date.getTime <= date_end.getTime) {
      arr += sdf.format(date)
      cd.setTime(date)
      //增加一天 放入集合
      cd.add(Calendar.MONTH, 1);
      date = cd.getTime
    }

    arr
  }

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

  /**
    * 读取每个月的在保人数
    *
    * @param sqlContext
    * @param location_mysql_url      odsdb数据库
    * @param prop                    访问mysql数据库
    * @param location_mysql_url_dwdb dwdb数据库
    * @return 返回企业每月某天的在保人数(在保人数最多的那天)
    **/
  def read_people_product(sqlContext: HiveContext, location_mysql_url: String, prop: Properties,
                          location_mysql_url_dwdb: String): RDD[(String, (Int, Int))] = {

    val now: Date = new Date()
    val dateFormatOne: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val now_Date = dateFormatOne.format(now)

    import sqlContext.implicits._
    val dim_1 = sqlContext.read.jdbc(location_mysql_url_dwdb, "dim_product", prop).select("product_code", "dim_1").where("dim_1 in ('外包雇主','骑士保','大货车')").map(x => x.getAs[String]("product_code")).collect

    val ods_policy_detail: DataFrame = sqlContext.read.jdbc(location_mysql_url, "ods_policy_detail", prop)
      .where("policy_status in ('1','0')")
      .select("ent_id", "policy_id", "insure_code")
      .filter("ent_id is not null")
    //    ods_policy_detail.foreach(println)

    val tep_ods_one = ods_policy_detail.map(x => (x.getAs[String]("insure_code"), x)).filter(x => if (dim_1.contains(x._1)) true else false)
      .map(x => {
        (x._2.getAs[String]("ent_id"), x._2.getAs[String]("policy_id"), x._2.getAs[String]("insure_code"))
      }).toDF("ent_id", "policy_id", "insure_code").cache

    //    tep_ods_one.foreach(println)
    var res = sqlContext.read.jdbc(location_mysql_url, "ods_policy_curr_insured", prop)
    val res_new = tep_ods_one.join(res, tep_ods_one("policy_id") === res("policy_id"), "left")

    val end = res_new.map(x => {
      ((x.getAs[String]("ent_id"), x.getAs[String]("day_id")), x.getAs[Long]("curr_insured").toInt)
    }).filter(_._1._2.toDouble == now_Date.toDouble)
    //    val end = sqlContext.read.jdbc(location_mysql_url, "ods_policy_curr_insured", prop).join(tep_ods_one, "policy_id").map(x => {
    //      ((x.getAs[String]("ent_id"), x.getAs[String]("day_id")), x.getAs[Long]("curr_insured").toInt)
    //    }).filter(_._1._2.toDouble == now_Date.toDouble)

    val end_ent_all = res_new.map(x => {
      ((x.getAs[String]("ent_id"), x.getAs[String]("day_id")), x.getAs[Long]("curr_insured").toInt)
    }).filter(_._1._2.toDouble == now_Date.toDouble)
      .map(x => {
        (x._1._1, "")
      }).distinct()

    //    end_ent_all.foreach(println)

    //    end.foreach(println)
    val end_all = end.reduceByKey(_ + _)

      .map(x => ((x._1._1, x._1._2.substring(0, 6)), x._2))
      .reduceByKey((x1, x2) => {
        if (x1 >= x2) x1 else x2
      }).map(x => (x._1._1, (x._1._2.toInt, x._2)))

    var res_last: RDD[(String, (String, Option[(Int, Int)]))] = end_ent_all.leftOuterJoin(end_all)

    var theEndRes: RDD[(String, (Int, Int))] = res_last.map(x => {
      var key = x._1.toString

      if (x._2._2.getOrElse("").toString.equals("")) {
        (key, (now_Date.substring(0, 6).toInt, "0".toInt))
      } else {
        (key, (x._2._2.get._1.toInt, x._2._2.get._2.toInt))
      }
    })

    //    theEndRes.foreach(println)
    //    println("---------")
    //    end_all.foreach(println)
    theEndRes
  }

  //遍历某目录下所有的文件和子文件
  def subDir(dir: File): Iterator[File] = {
    val dirs = dir.listFiles().filter(_.isDirectory())
    val files = dir.listFiles().filter(_.isFile())
    files.toIterator ++ dirs.toIterator.flatMap(subDir)
  }

  def getC3p0DateSource(path: String, table_name: String, url: String): Boolean = {
    Class.forName("com.mysql.jdbc.Driver")
    //获取连接//http://baidu.com
    val connection = DriverManager.getConnection(url)
    //通过连接创建statement
    var statement = connection.createStatement()
    val sql1 = s"truncate table odsdb.$table_name"
    val sql2 = s"load data infile '$path' replace into table odsdb.$table_name fields terminated by 'mk6'"
    statement = connection.createStatement()
    //先删除数据，在导入数据
    statement.execute(sql1)
    statement.execute(sql2)
  }

  //toMysql
  def toMsql(bzn_year: RDD[String], path_hdfs: String, path: String, table_name: String, url: String): Unit = {
    bzn_year.repartition(1).saveAsTextFile(path_hdfs)
    //得到我目录中的该文件
    val res_file = for (d <- subDir(new File(path))) yield if (d.getName.contains("-") && !d.getName.contains(".")) d.getName else "null"

    //得到part-0000
    val end = res_file.filter(_ != "null").mkString("")
    //通过load,将数据加载到MySQL中 : /share/ods_policy_insured_charged_vt/part-0000
    val tep_end = path + "/" + end
    getC3p0DateSource(tep_end, table_name, url)
  }

  //Hbase企业标签数据
  def getHbase_data(usersRDD: RDD[(ImmutableBytesWritable, Result)],
                    ods_ent_guzhu_salesman: Array[String]): RDD[(String, (String, Double, Double, Int, String))] = {

    val end: RDD[(String, (String, Double, Double, Int, String))] = usersRDD
      .map(x => {
        val s: (ImmutableBytesWritable, Result) = x
        //企业名称
        val ent_name = Bytes.toString(s._2.getValue("baseinfo".getBytes, "ent_name".getBytes))
        //如果最终赔付金额有值，就取最终金额，否则就取预估金额
        val pre_all_compensation = Bytes.toString(s._2.getValue("claiminfo".getBytes, "pre_all_compensation".getBytes))
        //已赚保费
        val charged_premium = Bytes.toString(s._2.getValue("claiminfo".getBytes, "charged_premium".getBytes))
        //连续在保月份
        val ent_continuous_plc_month = Bytes.toString(s._2.getValue("insureinfo".getBytes, "ent_continuous_plc_month".getBytes))

        //连续在保月份，每月显示
        val month_number = Bytes.toString(s._2.getValue("insureinfo".getBytes, "month_number".getBytes))

        val ent_name_tep_two = if (ent_name == null) null else ent_name.trim
        val pre_all_compensation_tep_two = if (pre_all_compensation == null) 0.0 else pre_all_compensation.replace(",", "").toDouble
        val charged_premium_tep_two = if (charged_premium == null) 0.0 else charged_premium.replace(",", "").toDouble
        val ent_continuous_plc_month_tep_two = if (ent_continuous_plc_month == null) 0 else ent_continuous_plc_month.toInt

        val month_number_tep_two = if (month_number == null) "0" else month_number


        val key = Bytes.toString(s._2.getRow) //读取rowKey
        (key, (ent_name_tep_two, pre_all_compensation_tep_two, charged_premium_tep_two, ent_continuous_plc_month_tep_two, month_number_tep_two))
      })
      .filter(x => if (x._2._1 != null && ods_ent_guzhu_salesman.contains(x._2._1)) true else false).persist(StorageLevel.MEMORY_ONLY)
    end
  }

  //得到最近渠道的在保月份，显示每月
  def show_months(month_sum_tep_one: String): ArrayBuffer[String] = {

    //最近的一次断开的月份，得到连续在保月份
    val res = month_sum_tep_one.split("-").sorted
    val first_data = res(0)
    val final_data = res(res.length - 1)
    //2个日期相隔多少个月，包括开始日期和结束日期
    val get_res_day = getBeg_End_one_two_month(first_data, final_data)

    //找出最近的一次的连续日期
    val filter_date = if (res.length == get_res_day.length) {
      month_add_jian(0, res(0))
    } else {
      val res_end = get_res_day.filter(!res.contains(_)).reverse(0)
      month_add_jian(1, res_end)
    }
    //得到2个日期之间相隔多少个月
    val end_final: ArrayBuffer[String] = getBeg_End_one_two_month(filter_date, final_data)
    end_final
  }

  def main(args: Array[String]): Unit = {
    val lines_source = Source.fromURL(getClass.getResource("/config-scala.properties")).getLines.toSeq
    val conf_s = new SparkConf().setAppName("membership_Level")
    //      .setMaster("local[4]")
    val prop: Properties = new Properties
    val sc = new SparkContext(conf_s)
    val sqlContext: HiveContext = new HiveContext(sc)
    val conf = HBaseConfiguration.create //hbase
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "172.16.11.106")
    conf.set(TableInputFormat.INPUT_TABLE, "labels:label_user_enterprise_vT")
    val usersRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    ) //读取hbase:得到：企业名称，已赚保费，金额，连续在保月份

    //当前企业的在保人数，貌似需要统计日期哎
    val location_mysql_url: String = lines_source(2).toString.split("==")(1)
    val location_mysql_url_dwdb: String = lines_source(10).toString.split("==")(1)

    //读取渠道表
    val ods_ent_guzhu_salesman: Array[String] = sqlContext.read.jdbc(location_mysql_url, "ods_ent_guzhu_salesman", prop)
      .map(x => x.getAs[String]("ent_name").trim)
      .distinct
      .collect
    val ods_ent_guzhu_salesman_channel = sqlContext.read.jdbc(location_mysql_url, "ods_ent_guzhu_salesman", prop)
      .map(x => {
        val ent_name = x.getAs[String]("ent_name").trim
        val channel_name = x.getAs[String]("channel_name")
        val new_channel_name = if (channel_name == "直客") ent_name else channel_name
        (ent_name, new_channel_name)
      })
      .filter(x => if (ods_ent_guzhu_salesman.contains(x._1)) true else false)
      .persist(StorageLevel.MEMORY_ONLY)

    //得到标签数据
    val hbase_result_data = getHbase_data(usersRDD: RDD[(ImmutableBytesWritable, Result)], ods_ent_guzhu_salesman: Array[String])

    //企业的每个月的在保人数
    val ent_people = read_people_product(sqlContext: HiveContext, location_mysql_url: String, prop: Properties, location_mysql_url_dwdb).persist(StorageLevel.MEMORY_ONLY)

    //标签数据与每个月的在保人数取值
    val tep_one_res = hbase_result_data
      .join(ent_people)
      .mapPartitions(par => {
        //将日期小于当前月的找出来
        val now: Date = new Date()
        val dateFormatOne: SimpleDateFormat = new SimpleDateFormat("yyyyMM")
        val now_Date = dateFormatOne.format(now)
        par.map(x => {
          //企业名称,  企业ID，金额，保费，连续在保月份,连续在保月份(每月显示) ,当前月份，当前月份的在保人数
          (x._2._1._1, (x._1, x._2._1._2, x._2._1._3, x._2._1._4, x._2._1._5, x._2._2._1.toDouble, x._2._2._2))
          //    (江西省聚力人力资源开发有限公司,(46509576a42747612159bd9c42eaeea8,0.0,7280.0,9,201705-201706-201707-201708-201709-201710-201711-201712-201801,201706,29))
        }).filter(_._2._6 <= now_Date.toDouble)
      })
      .cache.join(ods_ent_guzhu_salesman_channel)
      .map(x => {
        val total = x._2._1 //企业ID，金额，保费，连续在保月份,连续在保月份(每月显示) ,当前月份，当前月份的在保人数
        val quDao = x._2._2 //渠道
        val ent_name = x._1 //企业名称
        (quDao, (total, ent_name))
      })
      .groupByKey

    val tep_end = tep_one_res
      .map(x => {
        //计算渠道下的各个企业每个月的在保人数并 对其求和
        val month_ent = x._2
          .map(x => {
            val month = x._1._6.toDouble
            val month_people = x._1._7
            (month, month_people)
          })
          .groupBy(_._1)
          .map(x => (x._1, x._2.map(_._2).sum))

        //计算渠道下面所有企业的连续在保月数
        val month_sum_tep_one = x._2.map(x => x._1._5).reduce((x1, x2) => x1 + "-" + x2).split("-").distinct.mkString("-")
        val ent_continuous_plc_month = if (!month_sum_tep_one.contains("-") && month_sum_tep_one.length > 0) 1.0 else if (month_sum_tep_one.contains("-")) {
          //最近的一次断开的月份，得到连续在保月份
          val res = month_sum_tep_one.split("-").sorted
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
          val end_final = getBeg_End_one_two_month(filter_date, final_data).length.toDouble
          end_final
        } else 0.0

        //得到我渠道的连续在保月数--每月显示
        val tep_one_before = if (month_sum_tep_one != "0") show_months(month_sum_tep_one).mkString("-").split("-") else Array("")

        //得到最近渠道的在保月份，显示每月
        val partner_people_last = if (ent_continuous_plc_month >= 9.0) {
          val partner_people = month_ent.filter(x => tep_one_before.sorted.reverse.take(9).contains(x._1.toInt.toString)).map(x => {
            val end = if (x._2.toDouble >= 30000.0) "yes" else "no"
            end
          }).toArray
          partner_people
        } else Array("")
        val diamonds_people_last = if (ent_continuous_plc_month >= 6.0) {
          val diamonds_people = month_ent.filter(x => tep_one_before.sorted.reverse.take(6).contains(x._1.toInt.toString)).map(x => {
            val end = if (x._2.toDouble >= 8000.0 && x._2.toDouble < 30000.0) "yes" else "no"
            end
          }).toArray
          diamonds_people
        } else Array("")

        //取得最大的日期对应的在保人数(所有企业相加)--对应的也是渠道下所有企业的当前在保人数
        val max_month_people = month_ent.reduce((x1, x2) => if (x1._1 >= x2._1) x1 else x2)._2.toDouble
        val jiner = x._2.map(_._1._2.toDouble).toArray.sum //所有的金额
        val yizhauan = x._2.map(_._1._3.toDouble).toArray.sum //所有的已赚
        //已赔率=(预估赔付or实际赔付)/已赚保费*100%
        val reimbursement_rate = jiner / yizhauan

        val gold_yin_pu: (String, String, String) = if (ent_continuous_plc_month >= 9.0 && !partner_people_last.contains("no")) {
          val downgrade_he = if (reimbursement_rate >= 0.7 && reimbursement_rate < 1.0) "1" else if (reimbursement_rate >= 1.0) "2" else if (reimbursement_rate < 0.7) "0"
          (reimbursement_rate + "", "partner", downgrade_he + "")

        } else if (max_month_people >= 30000.0)
          (reimbursement_rate + "", "partner", "3")
        else if (ent_continuous_plc_month >= 6.0 && !diamonds_people_last.contains("no")) {
          val downgrade_zuan = if (reimbursement_rate >= 0.7 && reimbursement_rate < 1.0) "1" else if (reimbursement_rate >= 1.0) "2" else if (reimbursement_rate < 0.7) "0"
          (reimbursement_rate + "", "diamonds", downgrade_zuan + "")
        }
        else if (max_month_people >= 8000.0 && max_month_people < 30000.0)
          (reimbursement_rate + "", "diamonds", "3")

        else if (max_month_people >= 3000.0 && max_month_people < 8000.0) {
          val downgrade_gold = if (reimbursement_rate >= 0.75 && reimbursement_rate < 1.0) "1" else if (reimbursement_rate >= 1.0) "2" else if (reimbursement_rate < 0.75) "0"
          (reimbursement_rate + "", "gold", downgrade_gold + "")
        }
        else if (max_month_people >= 1000.0 && max_month_people < 3000) {
          val downgrade_yin = if (reimbursement_rate >= 0.75 && reimbursement_rate < 1.0) "1" else if (reimbursement_rate >= 1.0) "2" else if (reimbursement_rate < 0.75) "0"
          (reimbursement_rate + "", "silver", downgrade_yin + "")
        }
        else if (max_month_people > 0 && max_month_people < 1000) (reimbursement_rate + "", "ordinary", "0")
        else ("", "null", "")
        //渠道,赔付率,金额,已赚保费,会员等级，降级级别，该渠道下面所有企业的当前在保人数,该渠到企业的连续在保月数(所有企业累计)
        //      (x._1, (gold_yin_pu._1, jiner, yizhauan, gold_yin_pu._2, gold_yin_pu._3, max_month_people, ent_continuous_plc_month))

        //降级
        val real_member: String = if (gold_yin_pu._2 == "partner") {
          val real_member_partner = if (gold_yin_pu._3 == "0") "partner" else if (gold_yin_pu._3 == "1") "diamonds" else if (gold_yin_pu._3 == "2") "gold" else if (gold_yin_pu._3 == "3") "silver" else ""
          real_member_partner
        } else if (gold_yin_pu._2 == "diamonds") {
          val real_member_diamonds = if (gold_yin_pu._3 == "0") "diamonds" else if (gold_yin_pu._3 == "1") "gold" else if (gold_yin_pu._3 == "2") "silver" else if (gold_yin_pu._3 == "3") "ordinary" else ""
          real_member_diamonds
        } else if (gold_yin_pu._2 == "gold") {
          val real_member_gold = if (gold_yin_pu._3 == "0") "gold" else if (gold_yin_pu._3 == "1") "silver" else if (gold_yin_pu._3 == "2") "ordinary" else if (gold_yin_pu._3 == "3") "ordinary" else ""
          real_member_gold
        } else if (gold_yin_pu._2 == "silver") {
          val real_member_silver = if (gold_yin_pu._3 == "0") "silver" else if (gold_yin_pu._3 == "1") "ordinary" else if (gold_yin_pu._3 == "2") "ordinary" else if (gold_yin_pu._3 == "3") "ordinary" else ""
          real_member_silver
        } else if (gold_yin_pu._2 == "ordinary") "ordinary" else ""

        (s"${x._1}mk6${gold_yin_pu._1}mk6${jiner}mk6${yizhauan}mk6${gold_yin_pu._2}mk6${gold_yin_pu._3}mk6${max_month_people}mk6${ent_continuous_plc_month}mk6$real_member", tep_one_before.mkString("-"))
      })
      .mapPartitions(par => {
        //将日期小于当前月的找出来
        val now: Date = new Date
        val dateFormatOne: SimpleDateFormat = new SimpleDateFormat("yyyyMM")
        val now_Date = dateFormatOne.format(now)
        par.filter(_._2.split("-").contains(now_Date)).filter(_._1.split("mk6")(6).toDouble > 0.0)
      })
      .map(_._1)
    val table_name = "mid_guzhu_member_hierarchy"
    //得到时间戳
    val timeMillions = System.currentTimeMillis
    //HDFS需要传的路径
    val path_hdfs = s"file:///share/${table_name}_$timeMillions"
    //本地需要传的路径
    val path = s"/share/${table_name}_$timeMillions"
    //每天新创建一个目录，将数据写入到新目录中
    toMsql(tep_end, path_hdfs, path, table_name, location_mysql_url)
  }
}
