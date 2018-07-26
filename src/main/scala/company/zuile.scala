package company


import java.text.NumberFormat

import company.hbase_label.until
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object zuile extends until {

  val conf_spark = new SparkConf()
    .setAppName("wuYu")
  conf_spark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf_spark.registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
  conf_spark.set("spark.sql.broadcastTimeout", "36000").setMaster("local[2]")

  val sc = new SparkContext(conf_spark)
  val sqlContext: HiveContext = new HiveContext(sc)

  def main(args: Array[String]): Unit = {
    val dim_product = sqlContext.sql("select x.product_code from odsdb_prd.dim_product x where x.product_type_2='蓝领外包'").map(_.getAs("product_code").toString).collect()
    val bro = sc.broadcast(dim_product)

    val ods_policy_detail = sqlContext.sql("select ent_id,insure_code from odsdb_prd.ods_policy_detail").distinct()

    val ods_bro = ods_policy_detail.map(x => {
      val ent_id = x.getAs("ent_id").toString
      val insure_code = x.getAs("insure_code")
        .toString
      (ent_id, insure_code)
    }).filter(x => bro.value.contains(x._2))
      .map(_._1)
      .collect()

    val ods_bro_end = sc.broadcast(ods_bro)

    /**
      * 从Hbase中读取数据
      **/
    //    Spark读取HBase，我们主要使用SparkContext
    //    提供的newAPIHadoopRDDAPI将表的内容以 RDDs 的形式加载到 Spark 中。

    /**
      * 第一步:创建一个JobConf
      **/
    //定义HBase的配置
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "172.16.11.106")
    //设置查询的表名
        conf.set(TableInputFormat.INPUT_TABLE, "labels:label_user_enterprise_vT")
//    conf.set(TableInputFormat.INPUT_TABLE, "labels:label_user_enterprise")

    val usersRDD = sc.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    )
    //    val count = usersRDD.count()
    //    println("copy_m RDD Count" + count)

    val result = usersRDD.map { x => {
      val s: (ImmutableBytesWritable, Result) = x //69
      val pre_all_compensation = Bytes.toString(s._2.getValue("claiminfo".getBytes, "pre_all_compensation".getBytes))
      //x|男女比例 | 总人数
      //      (x, str, str_ent_scale)
      pre_all_compensation
    }}.filter(_!=null)
    result.foreach(println(_))


    println(result.count())

    //      .repartition(1).saveAsTextFile("F:\\tmp\\company\\ents")
  }
}
