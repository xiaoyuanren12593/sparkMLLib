package company.year.month

import java.io.{File, IOException}
import java.sql.DriverManager
import java.text.{NumberFormat, SimpleDateFormat}
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import java.util.{Calendar, Date}

import company.hbase_label.until
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MK on 2018/4/26.
  * 保费，精确到天，有年单的也有月单的
  */
object Year_Month_Premium extends year_until {


  //遍历某目录下所有的文件和子文件
  def subDir(dir: File): Iterator[File] = {
    val dirs = dir.listFiles().filter(_.isDirectory())
    val files = dir.listFiles().filter(_.isFile())
    files.toIterator ++ dirs.toIterator.flatMap(subDir)
  }

  def getC3p0DateSource(path: String): Boolean = {
    Class.forName("com.mysql.jdbc.Driver")
    //获取连接//http://baidu.com
    val connection = DriverManager.getConnection("jdbc:mysql://172.16.11.105:3306/odsdb?user=root&password=bzn@cdh123!")
    //通过连接创建statement
    var statement = connection.createStatement()
    //    val sql1 = "load data infile '/home/m.txt'  into table odsdb.ods_policy_insured_charged_vt"
    val sql1 = "truncate table odsdb.ods_policy_insured_charged_vt"

    val sql2 = s"load data infile '$path'  into table odsdb.ods_policy_insured_charged_vt"
    statement = connection.createStatement()
    //先删除数据，在导入数据
    statement.execute(sql1)
    statement.execute(sql2)
  }

  //toMysql
  def toMsql(bzn_year: RDD[(String, String, String, String, String, String, String, String, Double, String)], path_hdfs: String, path: String): Unit
  = {
    val to_mysql = bzn_year.map(x => s"${x._1}\t${x._2}\t${x._3}\t${x._4}\t${x._5}\t${x._6}\t${x._7}\t${x._8}\t${x._9}\t${x._10}")
    to_mysql.repartition(1).saveAsTextFile(path_hdfs)

    //得到我目录中的该文件
    val res_file = for (d <- subDir(new File(path))) yield {
      if (d.getName.contains("-") && !d.getName.contains(".")) d.getName else "null"
    }
    //得到part-0000
    val end = res_file.filter(_ != "null").mkString("")
    //通过load,将数据加载到MySQL中 : /share/ods_policy_insured_charged_vt/part-0000
    val tep_end = path + "/" + end
    getC3p0DateSource(tep_end)
  }

  //月单
  def month_premium(sqlContext: HiveContext, res: DataFrame, bp_bro: Broadcast[Array[String]]): RDD[(String, String, String, String, String, String, String, String, Double, String)]
  = {

    println("        "+bp_bro.value.length)

    val ods_policy_product_plan = sqlContext.sql("select * from odsdb_prd.ods_policy_product_plan").filter("length(sku_price)>0 and sku_charge_type=1").cache()

    val tep_one = res.join(ods_policy_product_plan, "policy_code")

    val bzn_year = tep_one.mapPartitions(rdd => {
      // 创建一个数值格式化对象(对数字)
      val numberFormat = NumberFormat.getInstance
      // 设置精确到小数点后2位
      numberFormat.setMaximumFractionDigits(4)

      rdd.flatMap(x => {
        val insure_code = x.getAs("insure_code").toString
        val policy_id = x.getAs("policy_id").toString
        val sku_price = x.getAs("sku_price").toString.toDouble
        val insured_id = x.getAs("insured_id").toString
        val insured_cert_no = x.getAs("insured_cert_no").toString

        val insured_start_date = x.getAs("insured_start_date").toString.split(" ")(0).replaceAll("-", "").replaceAll("/", "")
        val insured_end_date = x.getAs("insured_end_date").toString.split(" ")(0).replaceAll("-", "").replaceAll("/", "")

        val insure_policy_status = x.getAs("insure_policy_status").toString
        val ent_id = x.getAs("ent_id").toString
        // sku_charge_type:1是月单，2是年单子
        //判断是年单还是月单
        //如果是月单，则计算的是我当月的平均保费使用的字段是:insured_start_date,insured_end_date
        //如果是年单，则计算的是我当年的平均保费使用的字段是:start_date,end_date
        //每月人当月的天数如果为 1
        val month_number = getBeg_End_one_two(insured_start_date, insured_end_date).size

        val res = getBeg_End_one_two(insured_start_date, insured_end_date).map(day_id => {
          val sku_day_price = numberFormat.format(sku_price / month_number)
          (insure_code, policy_id, sku_day_price, insured_id, insured_cert_no, insured_start_date, insured_end_date, insure_policy_status, day_id, sku_price, ent_id)
        })
        res
      })
    }).filter(x => if (bp_bro.value.contains(x._1)) true else false)
    val to_hive = bzn_year.map(x => (x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9, x._10, x._11))
    to_hive
  }

  //年单
  def year_premium(sqlContext: HiveContext, res: DataFrame, bp_bro: Broadcast[Array[String]]): RDD[(String, String, String, String, String, String, String, String, Double, String)]
  = {

    val ods_policy_product_plan = sqlContext.sql("select * from odsdb_prd.ods_policy_product_plan").filter("length(sku_price)>0 and sku_charge_type=2").cache()

    val tep_one = res.join(ods_policy_product_plan, "policy_code")

    val bzn_year = tep_one.mapPartitions(rdd => {
      // 创建一个数值格式化对象(对数字)
      val numberFormat = NumberFormat.getInstance
      // 设置精确到小数点后2位
      numberFormat.setMaximumFractionDigits(4)

      rdd.flatMap(x => {
        val insure_code = x.getAs("insure_code").toString
        val policy_id = x.getAs("policy_id").toString
        val sku_price = x.getAs("sku_price").toString.toDouble
        val insured_id = x.getAs("insured_id").toString
        val insured_cert_no = x.getAs("insured_cert_no").toString

        val start_date = x.getAs("start_date").toString.split(" ")(0).replaceAll("-", "").replaceAll("/", "")
        val end_date = x.getAs("end_date").toString.split(" ")(0).replaceAll("-", "").replaceAll("/", "")

        val insured_start_date = x.getAs("insured_start_date").toString.split(" ")(0).replaceAll("-", "").replaceAll("/", "")
        val insured_end_date = x.getAs("insured_end_date").toString.split(" ")(0).replaceAll("-", "").replaceAll("/", "")

        val insure_policy_status = x.getAs("insure_policy_status").toString
        val ent_id = x.getAs("ent_id").toString
        // sku_charge_type:1是月单，2是年单子


        //判断是年单还是月单
        //如果是月单，则计算的是我当月的平均保费使用的字段是:insured_start_date,insured_end_date
        //如果是年单，则计算的是我当年的平均保费使用的字段是:start_date,end_date

        //保单层面的循环天数
        val date_number = getBeg_End_one_two(start_date, end_date).size


        val res = getBeg_End_one_two(insured_start_date, insured_end_date).map(day_id => {
          //          val sku_day_price = if (sku_charge_type == "1") numberFormat.format(sku_price / month_number) else numberFormat.format(sku_price / date_number)
          val sku_day_price = numberFormat.format(sku_price / date_number)
          (insure_code, policy_id, sku_day_price, insured_id, insured_cert_no, insured_start_date, insured_end_date, insure_policy_status, day_id, sku_price, ent_id)
        })
        res
      })
    }).filter(x => if (bp_bro.value.contains(x._1)) true else false)
    val to_hive: RDD[(String, String, String, String, String, String, String, String, Double, String)] = bzn_year.map(x => (x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9, x._10, x._11))
    to_hive

  }


  def main(args: Array[String]): Unit =   {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    //    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val conf_s = new SparkConf().setAppName("wuYu")
      .set("spark.sql.broadcastTimeout", "36000")
      .set("spark.network.timeout", "36000")
      .set("spark.executor.heartbeatInterval","20000")
          .setMaster("local[2]")
    val sc = new SparkContext(conf_s)
    val sqlContext: HiveContext = new HiveContext(sc)
    val ods_policy_detail = sqlContext.sql("select * from odsdb_prd.ods_policy_detail").filter("policy_status in ('0','1')").cache()
    val ods_policy_insured_detail = sqlContext.sql("select * from odsdb_prd.ods_policy_insured_detail").filter("length(insured_start_date)>0 and length(insured_end_date)>0")
    val blue_package = sqlContext.sql("select * from odsdb_prd.dim_product").filter("product_type_2='蓝领外包'")
    val bp = blue_package.map(x => x.getAs("product_code").toString).collect()
    val bp_bro: Broadcast[Array[String]] = sc.broadcast(bp)

    import sqlContext.implicits._

    /**
      * 存入hive
      **/
    val res: DataFrame = ods_policy_detail.join(ods_policy_insured_detail, "policy_id")

    val month = month_premium(sqlContext: HiveContext, res: DataFrame, bp_bro: Broadcast[Array[String]])
    val year = year_premium(sqlContext: HiveContext, res: DataFrame, bp_bro: Broadcast[Array[String]])
    val bzn_year: RDD[(String, String, String, String, String, String, String, String, Double, String)] = month.union(year)
    val to_hive = bzn_year.toDF("policy_id", "sku_day_price", "insured_id", "insured_cert_no", "insured_start_date", "insured_end_date", "insure_policy_status", "day_id", "sku_price", "ent_id")
    //我在hive中创建了一张 stored as  parquet的表，那么我直接存储即可
    //为true表示覆盖
    to_hive.insertInto("odsdb_prd.ods_policy_insured_charged_vt", overwrite = true)

    /*
      * 存入mysql
      **/

    //得到时间戳
    val timeMillions = System.currentTimeMillis()
    //HDFS需要传的路径
    val path_hdfs = s"file:///share/ods_policy_insured_charged_vt_$timeMillions"
    //本地需要传的路径
    val path = s"/share/ods_policy_insured_charged_vt_$timeMillions"

    //判断目录是否存在 ,存在的话则删除这个目录及其子目录
    val exis = new File(path)
    //每天新创建一个目录，将数据写入到新目录中
    if (exis.exists()) {

      val cmd = "sh /share/deleteFile.sh " + path + "/"
      val p = Runtime.getRuntime.exec(cmd)
      p.waitFor()
      if (p != null) {
        toMsql(bzn_year, path_hdfs, path)
      }
    } else {
      toMsql(bzn_year, path_hdfs, path)
    }
  }

}

