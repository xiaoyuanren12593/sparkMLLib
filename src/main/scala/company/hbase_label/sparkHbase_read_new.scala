package company.hbase_label

import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject


object sparkHbase_read_new {
  val sparkConf = new SparkConf().setAppName("HBaseTest")
    .setMaster("local[4]")
  val sc = new SparkContext(sparkConf)

  def KeyValueToString(keyValues: Array[KeyValue], json: JSONObject): String = {
    val it = keyValues.iterator
    val res = new StringBuilder
    while (it.hasNext) {
      val end = it.next()
      val row = Bytes.toString(end.getRow)
      val family = Bytes.toString(end.getFamily) //列族
      val qual = Bytes.toString(end.getQualifier) //字段
      val value = Bytes.toString(end.getValue) //字段值

      //      res.append(row + "->" + family + "->" + qual + "->" + value + ",")
      json.put("row", row)
      json.put("family", family)
      json.put("qual", qual)
      json.put("value", value)
      res.append(json.toString + ";")
    }
    res.substring(0, res.length - 1)
  }

  def main(args: Array[String]): Unit = {


    /**
      * 从Hbase中读取数据
      **/
    //        Spark读取HBase，我们主要使用SparkContext
    //        提供的newAPIHadoopRDDAPI将表的内容以 RDDs 的形式加载到 Spark 中。

    /**
      * 第一步:创建一个JobConf
      **/
    //定义HBase的配置
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "172.16.11.106")

    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "labels:label_user_enterprise_vT")


    val usersRDD = sc.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    )

    val a = sc.textFile("F:\\tmp\\优质客户名单.txt").collect

    val b = usersRDD.map(tuple => tuple._2)
      .map(result => {
        val name = Bytes.toString(result.getValue("baseinfo".getBytes, "ent_name".getBytes))
        (name, result.raw())
      }
      )
      .mapPartitions(rdd => {
        val json: JSONObject = new JSONObject
        rdd.map(f => {
          (f._1, KeyValueToString(f._2, json))
        })
      }).filter(x => a.contains(x._1))
      .foreach(x => println(x))
  }
}



