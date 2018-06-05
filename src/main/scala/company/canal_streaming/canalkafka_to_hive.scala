package company.canal_streaming

import com.alibaba.fastjson.JSON
import kafka.serializer.StringDecoder
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by MK on 2017/6/20.
  */

object canalkafka_to_hive {

  def createStreamingContext(): StreamingContext = {

    /**
      * init sparkStream couchbase kafka
      **/
    val conf = new SparkConf().setAppName("CouchbaseKafka")
//      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(10))
    ssc.checkpoint("hdfs://namenode1.cdh:8020/model_data/hive_four")

    val hiveContext = new HiveContext(sc)
    hiveContext.sql("set hive.exec.dynamic.partition.mode = nonstrict")

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

    val topicSet: Set[String] = Set("canal")
    val directKafka: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topicSet)
    val lines: DStream[(String, String)] = directKafka.map((x: (String, String)) => (x._1, x._2)) // kafka取出的数据，_1是其topic，_2是消息

    lines.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        //open_ofo_policy表的数据过滤出来，进行实时存储
        val ofo_column_name = Array(
          "policy_id",
          "proposal_no",
          "policy_no",
          "batch_id",
          "user_id",
          "product_code",
          "order_id",
          "start_date",
          "end_date",
          "holder_name",
          "holder_ename",
          "holder_first_name",
          "holder_last_name",
          "holder_mobile",
          "holder_cert_type",
          "holder_cert_no",
          "holder_email",
          "holder_birth_day",
          "holder_sex",
          "holder_industry",
          "insured_name",
          "insured_ename",
          "insured_first_name",
          "insured_last_name",
          "insured_mobile",
          "insured_cert_type",
          "insured_cert_no",
          "insured_holder_relation",
          "insured_email",
          "insured_birth_day",
          "insured_sex",
          "insured_industry",
          "status",
          "export_status",
          "create_time",
          "update_time",
          "month"
        )
        val tep_one = rdd.filter(x => if (x._1.contains("open_ofo_policy")) true else false).map(x => {
          val value = JSON.parseObject(x._2)
          Array(
            value.getString("policy_id"),
            value.getString("proposal_no"),
            value.getString("policy_no"),
            value.getString("batch_id"),
            value.getString("user_id"),
            value.getString("product_code"),
            value.getString("order_id"),
            value.getString("start_date"),
            value.getString("end_date"),
            value.getString("holder_name"),
            value.getString("holder_ename"),
            value.getString("holder_first_name"),
            value.getString("holder_last_name"),
            value.getString("holder_mobile"),
            value.getString("holder_cert_type"),
            value.getString("holder_cert_no"),
            value.getString("holder_email"),
            value.getString("holder_birth_day"),
            value.getString("holder_sex"),


            value.getString("holder_industry"),
            value.getString("insured_name"),
            value.getString("insured_ename"),
            value.getString("insured_first_name"),
            value.getString("insured_last_name"),
            value.getString("insured_mobile"),
            value.getString("insured_cert_type"),
            value.getString("insured_cert_no"),
            value.getString("insured_holder_relation"),
            value.getString("insured_email"),
            value.getString("insured_birth_day"),
            value.getString("insured_sex"),
            value.getString("insured_industry"),
            value.getString("status"),
            value.getString("export_status"),
            value.getString("create_time"),
            value.getString("update_time"),

            value.getString("month"))

        }).map(r => Row(r: _*))
        val schema = StructType(ofo_column_name.map(fieldName => StructField(fieldName, StringType, nullable = true)))
        val ofo_result = hiveContext.createDataFrame(tep_one, schema)
        ofo_result.write.format("parquet").partitionBy("month").mode(SaveMode.Append).saveAsTable("odsdb_prd.open_ofo_policy_vt")

        //open_other_policy 表的数据过滤出来，进行实时存储
        val tep_two = rdd.filter(x => if (x._1.contains("open_other_policy")) true else false).map(x => {
          val value = JSON.parseObject(x._2)
          Array(
            value.getString("policy_id"),
            value.getString("proposal_no"),
            value.getString("policy_no"),
            value.getString("batch_id"),
            value.getString("user_id"),
            value.getString("product_code"),
            value.getString("order_id"),
            value.getString("start_date"),
            value.getString("end_date"),
            value.getString("holder_name"),
            value.getString("holder_ename"),
            value.getString("holder_first_name"),
            value.getString("holder_last_name"),
            value.getString("holder_mobile"),
            value.getString("holder_cert_type"),
            value.getString("holder_cert_no"),
            value.getString("holder_email"),
            value.getString("holder_birth_day"),
            value.getString("holder_sex"),
            value.getString("holder_industry"),


            value.getString("insured_name"),
            value.getString("insured_ename"),
            value.getString("insured_first_name"),
            value.getString("insured_last_name"),
            value.getString("insured_mobile"),
            value.getString("insured_cert_type"),
            value.getString("insured_cert_no"),
            value.getString("insured_holder_relation"),
            value.getString("insured_email"),
            value.getString("insured_birth_day"),
            value.getString("insured_sex"),
            value.getString("insured_industry"),
            value.getString("status"),
            value.getString("export_status"),
            value.getString("create_time"),
            value.getString("update_time"),

            value.getString("month"))

        }).map(r => Row(r: _*))
        val other_column_name = Array(
          "policy_id",
          "proposal_no",
          "policy_no",
          "batch_id",
          "user_id",
          "product_code",
          "order_id",
          "start_date",
          "end_date",
          "holder_name",
          "holder_ename",
          "holder_first_name",
          "holder_last_name",
          "holder_mobile",
          "holder_cert_type",
          "holder_cert_no",
          "holder_email",
          "holder_birth_day",
          "holder_sex",
          "holder_industry",
          "insured_name",
          "insured_ename",
          "insured_first_name",
          "insured_last_name",
          "insured_mobile",
          "insured_cert_type",
          "insured_cert_no",
          "insured_holder_relation",
          "insured_email",
          "insured_birth_day",
          "insured_sex",
          "insured_industry",
          "status",
          "export_status",
          "create_time",
          "update_time",
          "month"
        )
        val schema_other = StructType(other_column_name.map(fieldName => StructField(fieldName, StringType, nullable = true)))
        val other_result = hiveContext.createDataFrame(tep_two, schema_other)
        other_result.write.format("parquet").partitionBy("month").mode(SaveMode.Append).saveAsTable("odsdb_prd.open_other_policy_vt")


        //open_express_policy 表的数据过滤出来，进行实时存储
        val tep_three = rdd.filter(x => if (x._1.contains("open_express_policy")) true else false).map(x => {
          val value = JSON.parseObject(x._2)
          Array(
            value.getString("policy_id"),
            value.getString("proposal_no"),
            value.getString("policy_no"),
            value.getString("user_id"),
            value.getString("order_id"),
            value.getString("start_time"),
            value.getString("end_time"),
            value.getString("delivery_province_code"),
            value.getString("delivery_city_code"),
            value.getString("delivery_area_code"),
            value.getString("delivery_address"),
            value.getString("shipping_province_code"),
            value.getString("shipping_city_code"),
            value.getString("shipping_area_code"),
            value.getString("shipping_address"),
            value.getString("client_mobile"),
            value.getString("courier_name"),


            value.getString("courier_mobile"),
            value.getString("courier_card_no"),
            value.getString("car_models"),
            value.getString("number_plate"),
            value.getString("good_type"),
            value.getString("amount"),
            value.getString("premium"),
            value.getString("product_type"),
            value.getString("company_name"),
            value.getString("status"),
            value.getString("export_status"),
            value.getString("create_time"),
            value.getString("update_time"),

            value.getString("month"))

        }).map(r => Row(r: _*))
        val express_column_name = Array(
          "policy_id",
          "proposal_no",
          "policy_no",
          "user_id",
          "order_id",
          "start_time",
          "end_time",
          "delivery_province_code",
          "delivery_city_code",
          "delivery_area_code",
          "delivery_address",
          "shipping_province_code",
          "shipping_city_code",
          "shipping_area_code",
          "shipping_address",
          "client_mobile",
          "courier_name",
          "courier_mobile",
          "courier_card_no",
          "car_models",
          "number_plate",
          "good_type",
          "amount",
          "premium",
          "product_type",
          "company_name",
          "status",
          "export_status",
          "create_time",
          "update_time",
          "month"
        )
        val schema_express = StructType(express_column_name.map(fieldName => StructField(fieldName, StringType, nullable = true)))
        val express_result = hiveContext.createDataFrame(tep_three, schema_express)
        express_result.write.format("parquet").partitionBy("month").mode(SaveMode.Append).saveAsTable("odsdb_prd.open_express_policy_vt")

        //open_coolqi_policy 表的数据过滤出来，进行实时存储
        val tep_four = rdd.filter(x => if (x._1.contains("open_coolqi_policy")) true else false).map(x => {
          val value = JSON.parseObject(x._2)
          Array(
            value.getString("policy_id"),
            value.getString("proposal_no"),
            value.getString("policy_no"),
            value.getString("batch_id"),
            value.getString("user_id"),
            value.getString("product_code"),
            value.getString("order_id"),
            value.getString("start_date"),
            value.getString("end_date"),
            value.getString("holder_name"),
            value.getString("holder_ename"),
            value.getString("holder_first_name"),
            value.getString("holder_last_name"),
            value.getString("holder_mobile"),
            value.getString("holder_cert_type"),
            value.getString("holder_cert_no"),
            value.getString("holder_email"),
            value.getString("holder_birth_day"),
            value.getString("holder_sex"),
            value.getString("holder_industry"),


            value.getString("insured_name"),
            value.getString("insured_ename"),
            value.getString("insured_first_name"),
            value.getString("insured_last_name"),
            value.getString("insured_mobile"),
            value.getString("insured_cert_type"),
            value.getString("insured_cert_no"),
            value.getString("insured_holder_relation"),
            value.getString("insured_email"),
            value.getString("insured_birth_day"),
            value.getString("insured_sex"),
            value.getString("insured_industry"),
            value.getString("status"),
            value.getString("export_status"),
            value.getString("create_time"),
            value.getString("update_time"),

            value.getString("month"))
        }).map(r => Row(r: _*))
        val coolqi_column_name = Array(
          "policy_id",
          "proposal_no",
          "policy_no",
          "batch_id",
          "user_id",
          "product_code",
          "order_id",
          "start_date",
          "end_date",
          "holder_name",
          "holder_ename",
          "holder_first_name",
          "holder_last_name",
          "holder_mobile",
          "holder_cert_type",
          "holder_cert_no",
          "holder_email",
          "holder_birth_day",
          "holder_sex",
          "holder_industry",
          "insured_name",
          "insured_ename",
          "insured_first_name",
          "insured_last_name",
          "insured_mobile",
          "insured_cert_type",
          "insured_cert_no",
          "insured_holder_relation",
          "insured_email",
          "insured_birth_day",
          "insured_sex",
          "insured_industry",
          "status",
          "export_status",
          "create_time",
          "update_time",
          "month"
        )
        val schema_coolqi = StructType(coolqi_column_name.map(fieldName => StructField(fieldName, StringType, nullable = true)))
        val coolqi_result = hiveContext.createDataFrame(tep_four, schema_coolqi)
        coolqi_result.write.format("parquet").partitionBy("month").mode(SaveMode.Append).saveAsTable("odsdb_prd.open_coolqi_policy_vt")
      }
    })
    ssc
  }

  /**
    * 读取配置文件的话，只能以传参的形式进行，无法在代码中实现读取本地conf
    **/
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
//    val ssc = StreamingContext.getOrCreate("hdfs://namenode1.cdh:8020/model_data/hive_four", createStreamingContext _)
    val ssc = StreamingContext.getOrCreate("/model_data/hive_four", createStreamingContext _)

    ssc.start()
    ssc.awaitTermination()
  }
}
