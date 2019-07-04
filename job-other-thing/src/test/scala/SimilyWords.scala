
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

object SimilyWords {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName(SimilyWords.getClass.getName)
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val sQLContext = new SQLContext(sc)

//    val hiveContext = new HiveContext(sc)

    //比较识别的保险公司和正确保险公司的相似度
    val tOcrVatinvoiceData = similarityMatch(sQLContext,"t_ocr_vatinvoice")
    //tOcrVatinvoiceData.show(10000)
    //结果写入到本地文件
    writeDataToLocal(tOcrVatinvoiceData)
    sc.stop()
  }

  def writeDataToLocal(data: RDD[(String, String, Double, Double, Double, String)]) = {
    data.repartition(1).saveAsTextFile("C:\\Users\\xingyuan\\Desktop\\Similarity1")
  }
  /**
    * 比较识别的保险公司和正确保险公司的相似度
    * @param sQLContext
    * @param table
    * @return
    */
  def similarityMatch(sQLContext:SQLContext,table:String): RDD[(String, String, Double, Double, Double, String)] ={
    import sQLContext.implicits._

    readMysqlTable(sQLContext,table)

    val  tOcrVatinvoiceData: RDD[(String, String, Double, Double, Double, String)] = readMysqlTable(sQLContext,table)
      .select("req_id","seller_name").where("seller_name is not null")

      .map(x => {
        val req_id = x.getAs[String]("req_id")
        val seller_name = x.getAs[String]("seller_name")
        println(seller_name)
        if(seller_name != null){
          val d = textCosine(seller_name,"中国人寿财产保险股份有限公司浙江省分公司")
          val d1 = textCosine(seller_name,"众安在线财产保险股份有限公司")
          val d2 = textCosine(seller_name,"中华联合财产保险股份有限公司北京分公司")
          val dMax = getMax(d,d1,d2)
          var resName = ""
          if(dMax == -1.0){
            (req_id,seller_name,d,d1,d2,"相似度有误")
          }else if(dMax == d){
            (req_id,seller_name,d,d1,d2,"中国人寿财产保险股份有限公司浙江省分公司")
          }else if(dMax == d1){
            (req_id,seller_name,d,d1,d2,"众安在线财产保险股份有限公司")
          }else {
            (req_id,seller_name,d,d1,d2,"中华联合财产保险股份有限公司北京分公司")
          }
        }else{
          (req_id,seller_name,1.0,1.0,1.0,"中华联合财产保险股份有限公司北京分公司")
        }
      }).map(x => x).filter(x => x._2.toString != "")
    tOcrVatinvoiceData
  }

  // 返回三个数中的最大值
  def getMax(a:Double, b:Double, c:Double): Double = {
    if (a > b) {
      if (a > c) {
        return a
      } else if (a == c) {
        return -1
      } else {
        return c
      }
    } else if (a == b) {
      if (c > a) {
        return c
      } else {
        return -1
      }
    } else {
      if (b > c) {
        return b
      } else if (b == c) {
        return -1
      } else {
        return c
      }
    }
  }

  /**
    * 获取 Mysql 表的数据
    *
    * @param sqlContext
    * @param tableName 读取Mysql表的名字
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlTable(sqlContext: SQLContext, tableName: String): DataFrame = {
    val properties: Properties = getProPerties()
    sqlContext
      .read
      .format("jdbc")
      .option("url", properties.getProperty("mysql.url"))
      .option("driver", properties.getProperty("mysql.driver"))
      .option("user", properties.getProperty("mysql.username"))
      .option("password", properties.getProperty("mysql.password"))
      .option("numPartitions","10")
      .option("partitionColumn","id")
      .option("lowerBound", "0")
      .option("upperBound","200")
      .option("dbtable", tableName)
      .load()
  }

  /**
    * 获取配置文件
    *
    * @return
    */
  def getProPerties() = {
    val lines_source = Source.fromURL(getClass.getResource("/config_scala.properties")).getLines.toSeq
    var properties: Properties = new Properties()
    for (elem <- lines_source) {
      val split = elem.split("==")
      val key = split(0)
      val value = split(1)
      properties.setProperty(key,value)
    }
    properties
  }

  /**
    * 求向量的模
    * @param vec
    * @return
    */
  def module(vec: Vector[Double]) = {
    math.sqrt(vec.map(math.pow(_, 2)).sum)
  }

  /**
    * 求两个向量的内积
    * @param v1
    * @param v2
    * @return
    */
  def innerProduct(v1: Vector[Double], v2: Vector[Double]) =
  {
    val listBuffer = ListBuffer[Double]()
    for (i <- 0 until v1.length; j <- 0 to v2.length; if i == j) {
      if (i == j)
        listBuffer.append(v1(i) * v2(j))
    }
    listBuffer.sum
  }

  /**
    * 求两个向量的余弦
    * @param v1
    * @param v2
    * @return
    */

  def cosvec(v1: Vector[Double], v2: Vector[Double]) = {
    val cos = innerProduct(v1, v2) / (module(v1) * module(v2))
    if (cos <= 1) cos else 1.0
  }

  /**
    * 余弦相似度
    * @param str1
    * @param str2
    * @return
    */
  def textCosine(str1: String, str2: String) = {
    val set = mutable.Set[Char]()
    // 不进行分词
    str1.foreach(set += _)
    str2.foreach(set += _)
    val ints1: Vector[Double] = set.toList.sorted.map(ch => {
      str1.count(s => s == ch).toDouble }).toVector
    val ints2: Vector[Double] = set.toList.sorted.map(ch => {
      str2.count(s => s == ch).toDouble
    }).toVector
    cosvec(ints1, ints2)
  }
}
