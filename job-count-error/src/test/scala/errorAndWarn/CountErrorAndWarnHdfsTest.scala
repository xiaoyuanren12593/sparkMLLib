package errorAndWarn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CountErrorAndWarnHdfsTest {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf_spark = new SparkConf()
      .setAppName("xingyuan")
      .setMaster("local[2]")
    conf_spark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf_spark.registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
    conf_spark.set("spark.sql.broadcastTimeout", "36000")
    conf_spark.set("fs.default.name","hdfs://datanode3.cdh:8020")


    val sc = new SparkContext(conf_spark)
    val res: RDD[String] = sc.textFile("hdfs://datanode3.cdh:8020")
    res.map(x => {
      val line = x.toString
      line
    }).foreach(println)
  }
}

