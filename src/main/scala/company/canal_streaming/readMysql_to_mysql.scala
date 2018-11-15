package company.canal_streaming

import java.util.Properties

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by MK on 2018/9/4.
  * 读取mysql中的bzn_open各个数据库中的表，存到ods数据库中
  */
object readMysql_to_mysql {
  def main(args: Array[String]): Unit = {
    val lines_source = Source.fromURL(getClass.getResource("/config_scala.properties")).getLines.toSeq
    val Neo4j_url = lines_source(7).toString.split("==")(1)
    val url11 = "jdbc:mysql://172.16.11.103:3306/bzn_open_201711?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&user=root&password=123456"
    val url12 = "jdbc:mysql://172.16.11.103:3306/bzn_open_201712?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&user=root&password=123456"
    val url1 = "jdbc:mysql://172.16.11.103:3306/bzn_open_201801?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&user=root&password=123456"
    val url2 = "jdbc:mysql://172.16.11.103:3306/bzn_open_201802?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&user=root&password=123456"
    val url3 = "jdbc:mysql://172.16.11.103:3306/bzn_open_201803?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&user=root&password=123456"
    val url4 = "jdbc:mysql://172.16.11.103:3306/bzn_open_201804?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&user=root&password=123456"
    val url5 = "jdbc:mysql://172.16.11.103:3306/bzn_open_201805?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&user=root&password=123456"
    val url6 = "jdbc:mysql://172.16.11.103:3306/bzn_open_201806?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&user=root&password=123456"
    val url7 = "jdbc:mysql://172.16.11.103:3306/bzn_open_201807?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&user=root&password=123456"
    val url8 = "jdbc:mysql://172.16.11.103:3306/bzn_open_201808?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&user=root&password=123456"
    val url9 = "jdbc:mysql://172.16.11.103:3306/bzn_open_201809?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&user=root&password=123456"
    System.setProperty("HADOOP_USER_NAME", "hdfs")

    val conf_s = new SparkConf().setAppName("wuYu").setMaster("local[4]")
    val sc = new SparkContext(conf_s)
    val sqlContext: HiveContext = new HiveContext(sc)
    val prop: Properties = new Properties

    val tep_one1 = sqlContext.read.jdbc(url11, "open_other_policy", prop).map(x => x.toSeq.mkString("mk"))
    val tep_one2 = sqlContext.read.jdbc(url12, "open_other_policy", prop).map(x => x.toSeq.mkString("mk"))
    val tep_one3 = sqlContext.read.jdbc(url1, "open_other_policy", prop).map(x => x.toSeq.mkString("mk"))
    val tep_one4 = sqlContext.read.jdbc(url2, "open_other_policy", prop).map(x => x.toSeq.mkString("mk"))
    val tep_one5 = sqlContext.read.jdbc(url3, "open_other_policy", prop).map(x => x.toSeq.mkString("mk"))
    val tep_one6 = sqlContext.read.jdbc(url4, "open_other_policy", prop).map(x => x.toSeq.mkString("mk"))
    val tep_one7 = sqlContext.read.jdbc(url5, "open_other_policy", prop).map(x => x.toSeq.mkString("mk"))
    val tep_one8 = sqlContext.read.jdbc(url6, "open_other_policy", prop).map(x => x.toSeq.mkString("mk"))
    val tep_one9 = sqlContext.read.jdbc(url7, "open_other_policy", prop).map(x => x.toSeq.mkString("mk"))
    val tep_one10 = sqlContext.read.jdbc(url8, "open_other_policy", prop).map(x => x.toSeq.mkString("mk"))
    val tep_one11 = sqlContext.read.jdbc(url9, "open_other_policy", prop).map(x => x.toSeq.mkString("mk")) //.repartition(1).saveAsTextFile("C:\\Users\\a2589\\Desktop\\hehe")
    tep_one1.union(tep_one2).union(tep_one3).union(tep_one4).union(tep_one5).union(tep_one6).union(tep_one7).union(tep_one8)
      .union(tep_one9).union(tep_one10).union(tep_one11).repartition(1).saveAsTextFile("C:\\Users\\a2589\\Desktop\\hehe")
  }
}
