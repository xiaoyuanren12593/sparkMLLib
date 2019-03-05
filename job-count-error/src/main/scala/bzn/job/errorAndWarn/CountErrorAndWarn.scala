package bzn.job.errorAndWarn

import org.apache.spark.{SparkConf, SparkContext}

object CountErrorAndWarn {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf_spark = new SparkConf()
      .setAppName("xingyuan")
    conf_spark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf_spark.registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
    conf_spark.set("spark.sql.broadcastTimeout", "36000")
      .setMaster("local[2]")

    val sc = new SparkContext(conf_spark)

    val read = sc.textFile("D:\\git_repository\\sparkMLlib\\job-count-error\\src\\test\\scala\\Data\\error-iface\\*.log")
    read.foreach(println)

    sc.stop()
  }
}
