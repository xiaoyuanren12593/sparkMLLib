package bzn.job.etl

import java.io.File
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MK on 2018/6/15.
  * 对小于当天的数据进行计算，同时对保单号进行分组。并将每天的保费进行相加
  */
object YearMonthPremiumPolicyIdTest {

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
    val sql1 = "truncate table odsdb.ods_policy_charegd_day"

    val sql2 = s"load data infile '$path'  into table odsdb.ods_policy_charegd_day"
    statement = connection.createStatement()
    //先删除数据，在导入数据
    statement.execute(sql1)
    statement.execute(sql2)
  }

  //得到当前的日期
  def get_now_time: Int = {
    val now: Date = new Date()
    val dateFormatOne = new SimpleDateFormat("yyyyMMdd")
    dateFormatOne.format(now).toInt
  }

  //toMysql
  def toMsql(bzn_year: RDD[(String, Double)], path_hdfs: String, path: String): Unit = {
    val to_mysql = bzn_year.map(x => s"${x._1}\t${x._2.formatted("%.4f")}").repartition(1).saveAsTextFile(path_hdfs)

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


  def main(args: Array[String]): Unit = {

    val conf_s = new SparkConf().setAppName("wuYu")
      .setMaster("local[2]")
    val sc = new SparkContext(conf_s)
    val sqlContext: HiveContext = new HiveContext(sc)

    val ods_policy_insured_charged_vt = sqlContext.sql("select * from odsdb_prd.ods_policy_insured_charged_vt")

    val bzn_year: RDD[(String, Double)] = ods_policy_insured_charged_vt
      .map(x => {
        val policy_Id = x.getAs[String]("policy_id")
        val sku_day_price = x.getAs[String]("sku_day_price")
        val day_id = x.getAs[String]("day_id")
        (policy_Id, sku_day_price, day_id)
      })
      .filter(_._3.toInt <= get_now_time)
      .map(x => (x._1, x._2.toDouble))
      .reduceByKey(_ + _)

    /**
      * 存入mysql中
      **/
    //得到时间戳
    val timeMillions = System.currentTimeMillis()
    //HDFS需要传的路径
    val path_hdfs = s"file:///share/ods_policy_chared_day_$timeMillions"
    //本地需要传的路径
    val path = s"/share/ods_policy_chared_day_$timeMillions"
    //判断目录是否存在 ,存在的话则删除这个目录及其子目录
    val exis = new File(path)
    //每天新创建一个目录，将数据写入到新目录中
    if (exis.exists()) {
      val cmd = "sh /share/deleteFile.sh " + path + "/"
      val p = Runtime.getRuntime.exec(cmd)
      p.waitFor()
      if (p != null) {
        bzn_year.foreach(x => println(x))
        //        toMsql(bzn_year, path_hdfs, path)
      }
    } else {
      bzn_year.foreach(x => println(x))
      //      toMsql(bzn_year, path_hdfs, path)
    }
  }
}
