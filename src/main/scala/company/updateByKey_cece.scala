package company

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by MK on 2018/6/20.
  */
object updateByKey_cece {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")

    /**
      * init sparkStream couchbase kafka
      **/
    val conf = new SparkConf().setAppName("CouchbaseKafka")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint("hdfs://namenode1.cdh:8020/streaming_ceshi")

    /**
      * kafka conf
      **/
    val kafkaParam: Map[String, String] = Map[String, String](
      //-----------kafka低级api配置-----------
      "zookeeper.connect" -> "namenode2.cdh:2181,datanode3.cdh:2181,namenode1.cdh:2181", //----------配置zookeeper-----------
      "metadata.broker.list" -> "namenode1.cdh:9092",
      "group.id" -> "canal_kafka", //设置一下group id
      "auto.offset.reset" -> kafka.api.OffsetRequest.LargestTimeString, //----------从该topic最新的位置开始读数------------
      "client.id" -> "canal_kafka",
      "zookeeper.connection.timeout.ms" -> "10000"
    )
    val addFunc = (currValues: Seq[String], prevValueState: Option[String]) => {
      //通过Spark内部的reduceByKey按key规约，然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
      //      prevValueState
      val previousCount = prevValueState.getOrElse(0)
      val ss = currValues.mkString(",")
      // 返回累加后的结果，是一个Option[Int]类型
      //      val s = ss +","+ previousCount
      Some(ss + "," + previousCount)
    }
    val topicSet: Set[String] = Set("test-kevin")
    val directKafka: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topicSet)
    val lines: DStream[(String, String)] = directKafka.map((x: (String, String)) => (x._1, x._2)) // kafka取出的数据，_1是其topic，_2是消息

    val words = lines.map(x => {
      val s = x._2.split(",")
      (s(1), s(0))
    }).reduceByKey((x1, x2) => {
      x1 + "," + x2
    })

    //.foreachRDD(rdd => rdd.foreach(x => println(s"${x._1}:${x._2}")))

    words.updateStateByKey[String](addFunc)
      .foreachRDD(rdd => rdd.foreach(println(_)))
    ssc.start()
    ssc.awaitTermination()

  }
}
