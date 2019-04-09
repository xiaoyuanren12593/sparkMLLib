package ofo_person_Analysis

import com.alibaba.fastjson.JSON
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by MK on 2018/6/22.
  */
object Analysis {

  val conf_spark = new SparkConf()
    .setAppName("wuYu")
  conf_spark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf_spark.registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
  conf_spark.set("spark.sql.broadcastTimeout", "36000")
    .setMaster("local[2]")

  val sc = new SparkContext(conf_spark)

  val sqlContext: HiveContext = new HiveContext(sc)
  /**
    * 第一步:创建一个JobConf
    **/
  //定义HBase的配置
  val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  conf.set("hbase.zookeeper.quorum", "172.16.11.106")

  //设置查询的表名
  conf.set(TableInputFormat.INPUT_TABLE, "labels:label_user_personal_vT")

  val usersRDD = sc.newAPIHadoopRDD(conf,
    classOf[TableInputFormat],
    classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
    classOf[org.apache.hadoop.hbase.client.Result]
  )

  def main(args: Array[String]): Unit = {

    //得到省份
    //d_cant	省市区码表	省市区信息
    val d_cant = sqlContext.sql("select * from odsdb_prd.d_cant").filter("length(name)>0").cache

    //通过省区的名字，找出对应的code
    val cert_native_province_r = d_cant.map(x => {
      val code = x.getString(0)
      val name = x.getString(1)
      (name, code)
    }).filter(_._2.contains("0000")).collectAsMap

    val result = usersRDD.map { x => {
      val s: (ImmutableBytesWritable, Result) = x
      val ent_man_woman_proportion = Bytes.toString(s._2.getValue("goout".getBytes, "user_person_only_ofo".getBytes))
      ent_man_woman_proportion
    }
    }.filter(_ != null).mapPartitions(rdd => {
      rdd.map(x => {
        val json_arr = JSON.parseObject(x)
        val all = json_arr.getString("user_information").split(",")
        val get_Province = all(3)
        //得到对应省份的代码编号
        val province_code = cert_native_province_r.getOrElse(get_Province, "0.00").toDouble
        //得到骑行频率
        val frequency = if (json_arr.get("riding_frequency_res") == null) 0.00 else json_arr.getString("riding_frequency_res").toDouble
        //投保次数
        val user_insure_product_num = if (json_arr.get("number_product") == null) 0.00 else json_arr.getString("number_product").toDouble
        Vectors.dense(province_code, frequency, user_insure_product_num)
      })
    })

  }
}
