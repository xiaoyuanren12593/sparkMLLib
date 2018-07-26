package company.sparkMLlib.another

import java.util.regex.Pattern

import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.DicAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


object za_Standard {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)

  Logger.getLogger("org").setLevel(Level.OFF)
  System.setProperty("spark.ui.showConsoleProgress", "False")


  //正则提取
  def tq(str_math: String): (String, String) = {
    //提取数字
    val regEx = "[^0-9]"
    val p = Pattern.compile(regEx)
    val m = p.matcher(str_math)
    val math_str = m.replaceAll("").trim
    //提取汉字
    val reg: String = "[^\u4e00-\u9fa5]"
    val tr: String = str_math.replaceAll(reg, "")
    (math_str, tr)
  }

  //统计某一元素在数组中出现的次数
  def findCount(a: Array[Int], len: Int, key: Int): Int = {
    var i = 0
    var count = 0
    i = 0
    while ( {
      i < len
    }) {
      if (key == a(i)) {
        count += 1;
        count - 1
      }

      {
        i += 1;
        i - 1
      }
    }
    count
  }

  //算法统计相同
  def bruteForceStringMatch(source: String, pattern: String): Int = {
    val slen = source.length
    val plen = pattern.length
    val s = source.toCharArray
    val p = pattern.toCharArray
    var i = 0
    var j = 0
    if (slen < plen) -1 //如果主串长度小于模式串，直接返回-1，匹配失败
    else {
      while ( {
        i < slen && j < plen
      }) if (s(i) == p(j)) { //如果i,j位置上的字符匹配成功就继续向后匹配
        i += 1
        j += 1
      }
      else {
        i = i - (j - 1) //i回溯到主串上一次开始匹配下一个位置的地方

        j = 0 //j重置，模式串从开始再次进行匹配

      }
      if (j == plen) { //匹配成功
        i - j
      }
      else -1 //匹配失败
    }
  }

  case class d_worktype_za(id: String, value: String)

  case class temp_worktype_20180316(name: String)


  def main(args: Array[String]): Unit = {
    //    System.setProperty("hadoop.home.dir", "H:\\spark_JiQun\\hadoop-2.6.0")
    //分词准备
    val stop = new StopRecognition()
    stop.insertStopNatures("w") //过滤掉标点
    val conf = new SparkConf().setAppName("Vector")
      .setMaster("local[2]")
    //      .set("hive.metastore.uris", "thrift://namenode1.cdh:9083")
    //远程链接需要导入相对应的3个包

    val sc = new SparkContext(conf)
    val sql = new HiveContext(sc)

    import sql.implicits._
    val splited = sql.sql("select id,stype as d_za_value from d_worktype_za")
    val splited_one = splited.flatMap(x => {

      val str = x(1).toString
      val result_str = DicAnalysis.parse(str).recognition(stop).toStringWithOutNature(s"\t${x(0)}")
      val res = "".concat(s"${x(0)}").concat(result_str).concat("")
      val next_value = res.split("\t")
      next_value
    }).map(next_value => {
      d_worktype_za(tq(next_value)._1, tq(next_value)._2)
    }).toDF
    val split_two = splited_one.join(splited, "id")
    //        split_two.show()
    //    | id | value | d_za_value |
    //    |0001001|   内勤|              内勤人员|
    //读取文本对每一个行数据进行分词
    val splits_ods = sql.sql("select name from temp_worktype_20180316")

    val za_data = split_two.map(x => {
      //分词，id，原属数据
      (x(1) + "".replaceAll("工人", "").replaceAll("人员", ""), (x(0) + "", x(2) + ""))
    })

    val sbaf_data: RDD[(String, String)] = splits_ods.map(s => {
      val data_testsentence_ods = DicAnalysis.parse(s(0).toString)
        .recognition(stop)
        .toStringWithOutNature("-")
      (data_testsentence_ods.mkString(""), s)
    }) .flatMap(x => {
      //提取汉字，并去除空格，在去除标点符号
      val keys = x._1.split("-").map(s => s"${tq(s)._2.replace(" ", "").replaceAll("[\\pP\\p{Punct}]", "")}:${x._2}")
      keys
    }).map(x => {
      val res = x.split(":")
      (res(0), res(1) replaceAll("[\\pP\\p{Punct}]", ""))
    })

    val sc_Res = za_data.join(sbaf_data).map(x => {
      //      (((0803037,原电池制造工人),电池挂件),-1)
      //      (((0803037,原电池制造工人),电池挂机),-1)
      //      (((0803037,原电池制造工人),汽车电池搬运工),-1)
      //      (((0803038,热电池制造工人),电池制造工人),热电池制造工人)
      //      (((0803038,热电池制造工人),电池挂件搬运小成品),-1)
      //众安原始数据
      val za = x._2._1._2
      //输入数据
      val sr = x._2._2
      val res = bruteForceStringMatch(za, sr)
      val result = if (res != -1) za + "" else -1 + ""
      (x._2._1._1, x._2._1._2, x._2._2, result)
    })


    sc_Res.toDF("id", "d_za", "input_name", "name").insertInto("result_full")
    sc.stop()
  }
}
