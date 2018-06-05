//package com.lava.kafka
//
//import java.text.SimpleDateFormat
//import java.util
//import java.util.Date
//
//
//import kafka.serializer.StringDecoder
//import org.apache.kafka.clients.consumer.ConsumerConfig
//import org.apache.spark.sql.SaveMode
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.{SparkContext, SparkConf}
//
///**
//  * Created by
//  */
//object StreamAppUserData {
//
//  def main(args: Array[String]) {
//
//
//
//
//    val Array(file,brokers, group, topics) = args
//
//    val topicSet = topics.split(",").toSet
//    val kafkaParams = Map[String, String](
//      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG  -> brokers,
//      ConsumerConfig.GROUP_ID_CONFIG -> group
//    )
//    val km = new KafkaManager(kafkaParams)
//    val properties: util.List[String] = JsonUtil.readProperties(file)
//    val sparkConf = new SparkConf().set("spark.streaming.stopGracefullyOnShutdown","true") //.setAppName(file).setMaster("local[2]")
//
//    val sc = new SparkContext(sparkConf)
//    val ssc = new StreamingContext(sc, Seconds(60))
//    val hiveContext = new HiveContext(sc)
//    import hiveContext.implicits._
//    hiveContext.setConf("hive.exec.dynamic.partition", "true")
//    hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
//    //    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
//    //    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
//    val lines = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)
//    lines.foreachRDD(rdd => {
//      if (!rdd.isEmpty()) {
//        // 先处理消息
//        val words = rdd.map(_._2).flatMap(JsonUtil.JsonToList(_,properties).toArray)
//        //        words.collect().foreach(println)
//        val time = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
//        import com.lava.kafka.Dome.Appuserdata
//        import com.lava.kafka.Dome.ErrorData
//        words.map(x=>x.toString.split("\001")).filter(x=>x.length==13).map(x=>Appuserdata(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12),time)).
//          toDF().write.partitionBy("date").mode(SaveMode.Append).saveAsTable("tb_lava_appuserdata")
//        words.map(x=>x.toString.split("\001")).filter(x=>x.length!=13).map(x=>ErrorData("appuserdata",x.mkString("\001"),time))
//          .toDF().write.partitionBy("date").mode(SaveMode.Append).saveAsTable("tb_lava_errordata")
//        // 再更新offsets
//        km.updateZKOffsets(rdd)
//      }
//    })
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//
//
//
//}