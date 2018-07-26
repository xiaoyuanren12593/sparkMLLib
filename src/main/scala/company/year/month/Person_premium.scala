package company.year.month

import java.io.File
import java.sql.DriverManager

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by MK on 2018/6/21.
  */
object Person_premium {
  //遍历某目录下所有的文件和子文件
  def subDir(dir: File): Iterator[File]
  = {
    val dirs = dir.listFiles().filter(_.isDirectory())
    val files = dir.listFiles().filter(_.isFile())
    files.toIterator ++ dirs.toIterator.flatMap(subDir)
  }

  def getC3p0DateSource(path: String)
  = {
    Class.forName("com.mysql.jdbc.Driver")
    //获取连接//http://baidu.com
    val connection = DriverManager.getConnection("jdbc:mysql://172.16.11.105:3306/odsdb?user=root&password=bzn@cdh123!")
    //通过连接创建statement
    var statement = connection.createStatement()
    //    val sql1 = "load data infile '/home/m.txt'  into table odsdb.ods_policy_insured_charged_vt"
    val sql1 = "truncate table odsdb.ods_policy_charged_day_mid_vt"

    val sql2 = s"load data infile '$path'  into table odsdb.ods_policy_charged_day_mid_vt"
    statement = connection.createStatement()
    //先删除数据，在导入数据
    statement.execute(sql1)
    statement.execute(sql2)
  }

  //toMysql
  def toMsql(bzn_year: RDD[(String, String, Double)], path_hdfs: String, path: String): Unit
  = {
    val to_mysql = bzn_year.map(x => s"${x._1}\t${x._2}\t${x._3.formatted("%.4f")}").repartition(1).saveAsTextFile(path_hdfs)
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
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf_s = new SparkConf().setAppName("wuYu")
    //      .setMaster("local[2]")
    val sc = new SparkContext(conf_s)
    val sqlContext: HiveContext = new HiveContext(sc)
    val ods_policy_insured_charged_vt = sqlContext.sql("select * from odsdb_prd.ods_policy_insured_charged_vt")

    val bzn_year: RDD[(String, String, Double)] = ods_policy_insured_charged_vt.map(x => {
      val policy_id = x.getAs[String]("policy_id")
      val day_id = x.getAs[String]("day_id")
      val sku_day_price = x.getAs[String]("sku_day_price")
      ((policy_id, day_id), sku_day_price.toDouble)
    }).reduceByKey(_ + _).map(x => (x._1._1, x._1._2, x._2))

    /**
      * 存入mysql中
      **/
    //得到时间戳
    val timeMillions = System.currentTimeMillis()
    //HDFS需要传的路径
    val path_hdfs = s"file:///share/ods_policy_charged_day_mid_vt_$timeMillions"
    //本地需要传的路径
    val path = s"/share/ods_policy_charged_day_mid_vt_$timeMillions"

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
