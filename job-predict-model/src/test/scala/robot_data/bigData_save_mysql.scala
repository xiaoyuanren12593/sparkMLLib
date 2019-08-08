package robot_data

import java.util.Properties

import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.DicAnalysis
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by MK on 2018/9/13.
  */
object bigData_save_mysql {


  def one (sc: SparkContext): Unit ={
    //    val tep_Twos = sqlContext.read.jdbc("jdbc:mysql://172.16.11.105:3306/robotdb?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&user=root&password=bzn@cdh123!", "robot_data", prop)
    //
    //    tep_Twos.map(x=>{
    //      val answer = x.getAs[String]("answer")
    //      (x,answer)
    //    }).filter(_._2.contains("我们需要了解失败")).map(_._1).foreach(println(_))

    val s = sc.wholeTextFiles("C:\\Users\\a2589\\OneDrive\\World文档\\保准牛\\AI机器人\\机器人超级话美女版.txt")
    s
      .map(x => (x._1, x._2.replace("[", "").replace("]", "")))
      .map(x => x._2.split("\r\n").mkString("&"))
      .flatMap(x => {
        x.split("&&")
      }).map(x => {
      val before = x.split("&")

      import java.util.regex.Pattern
      val p = Pattern.compile("[\u4e00-\u9fa5]")
      val m = p.matcher(before(1))
      val value = if (m.find()) before(1).replaceAll("[a-zA-Z]", "").replaceAll("[\\d]", "") else before(1)


      s"&默认&${before(0)}&&$value&1&&&&&"
    }).repartition(1).saveAsTextFile("C:\\Users\\a2589\\OneDrive\\World文档\\保准牛\\AI机器人\\AI.txt")

  }
  def main(args: Array[String]): Unit = {
    val conf_s = new SparkConf().setAppName("wuYu").setMaster("local[4]")
    val sc: SparkContext = new SparkContext(conf_s)
    val sqlContext: HiveContext = new HiveContext(sc)
    val prop: Properties = new Properties

val s= "a,d,c"
    s.map(x=>s.split(",").flatten)
  }
}
