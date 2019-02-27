package bzn.job.etl

import java.util.Properties

import bzn.job.until.RedisUntil
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import kafka.serializer.StringDecoder
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.DicAnalysis
import org.apache.log4j
import org.apache.log4j.Level
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.driver.v1._
import redis.clients.jedis.Jedis

import scala.io.Source


/**
  * Created by MK on 2018/7/30
  * 创建机器人回话的同时也要处理工种类型
  **/

object RobotStreaming extends RedisUntil {
  /**
    * 机器人工种回答
    **/
  def robot_work_type(x: (String, String), loggers: log4j.Logger, stop: StopRecognition, filter_directory: Seq[String],
                      normal_classification_of_work: Array[String], ods_policy_worktype_map: Array[String],
                      json_topic_end_work: JSONObject): (String, String) = {

    val json_topic_work = JSON.parseObject(x._2)
    loggers.info("------接受到了kafka的数据------" + json_topic_work)
    val msgId = json_topic_work.getString("msgId")

    val insuranceCompany = json_topic_work.getString("insuranceCompany")
    val str = json_topic_work.getString("searchStr")

    //过滤掉常用词汇,但也有全部过滤掉的风险，因此在这里我加了个判断
    val filter_value = Array("工人", "人员")
    val before_data_kafka = DicAnalysis.parse(str).recognition(stop).toStringWithOutNature(" ")
    val after_data_kafka = DicAnalysis.parse(str).recognition(stop).toStringWithOutNature(" ")
      .split(" ")
      .map(_.trim)
      .filter(!filter_directory.contains(_))
      .filter(!filter_value.contains(_))
    val result_str_work: String = if (after_data_kafka.length == 0) before_data_kafka else after_data_kafka.mkString(" ")

    val robot_data_json_work_type = normal_classification_of_work.filter(x => {
      val name = JSON.parseObject(x).getString("insurance_company")
      if (insuranceCompany.contains(name) || insuranceCompany == name || name.contains(insuranceCompany)) true else false
    })
    val end_case_work = getProduct_result_work(robot_data_json_work_type, result_str_work, stop)
    //找到我数据集合中，举重最大的值
    val end_max = if (end_case_work.length < 1) 0.0 else end_case_work.map(x => ((x._1, x._2), x._3.toDouble)).reduce((x1, x2) => if (x1._2 > x2._2) x1 else x2)._2

    //得到结果且只会是一个在我标准类中
    val just_one: String = end_case_work.filter(_._3.toDouble == end_max).take(1).mkString("").replace("(", "").replace(")", "")

    val before_just_one_jsonObject = new JSONObject
    //在我的对应表中找到对应的关系
    val end_case_work_mapping: Array[(String, Int, String, String)] = getProduct_result_work_mapping(ods_policy_worktype_map, result_str_work, stop)
    val work_type_mapp: JSONArray = getAnswer_work(stop, just_one, end_case_work_mapping)

    //如果我输入的数据在我标准表中完全存在，则就没必要走下面的分词权重了，但是匹配表还是要走的
    val all_before = getProduct_result_work_all(robot_data_json_work_type: Array[String], str: String)

    val end_json_topic_work = if (all_before.length > 0) {
      val all_json = all_before.reduce((x1, x2) => if (x1._2 <= x2._2) x1 else x2) //取得答案长度最小的
      val all_value = JSON.parseObject(all_json._1._2)

      before_just_one_jsonObject.put("workName", all_value.getString("worktype_name"))
      before_just_one_jsonObject.put("workCode", all_value.getString("id"))
      before_just_one_jsonObject.put("lType", all_value.getString("profession"))
      before_just_one_jsonObject.put("workLevel", all_value.getString("ai_level"))
      before_just_one_jsonObject.put("insuranceCompany", all_value.getString("insurance_company"))

      json_topic_end_work.put("msgId", msgId)
      json_topic_end_work.put("standardWork", before_just_one_jsonObject)
      json_topic_end_work.put("mapingWorks", work_type_mapp)
      json_topic_end_work
    } else if (just_one != "" && work_type_mapp.size() >= 1) {
      before_just_one_jsonObject.put("workName", just_one.split(",")(0))
      before_just_one_jsonObject.put("workCode", just_one.split(",")(3))
      before_just_one_jsonObject.put("lType", just_one.split(",")(4))
      before_just_one_jsonObject.put("workLevel", just_one.split(",")(5))
      before_just_one_jsonObject.put("insuranceCompany", just_one.split(",")(6))

      json_topic_end_work.put("msgId", msgId)
      //      json_topic_end_work.put("standardWork", before_just_one_jsonObject)
      json_topic_end_work.put("standardWork", null)
      json_topic_end_work.put("mapingWorks", work_type_mapp)
      json_topic_end_work
    } else if (just_one == "" && work_type_mapp.size() >= 1) {
      json_topic_end_work.put("msgId", msgId)
      json_topic_end_work.put("standardWork", null)
      json_topic_end_work.put("mapingWorks", work_type_mapp)
      json_topic_end_work
    } else {
      json_topic_end_work.put("msgId", msgId)
      json_topic_end_work.put("standardWork", null)
      json_topic_end_work.put("mapingWorks", null)
      json_topic_end_work
    }

    loggers.info(s"打印数据,同时将数据输出到redis中------${end_json_topic_work.toJSONString}")
    val key = s"worktype_search_$msgId"
    (key, end_json_topic_work.toJSONString)

  }

  /**
    * 机器人正常回话
    **/
  def robot(stop: StopRecognition, x: (String, String), loggers: log4j.Logger,
            filter_directory: Seq[String], robot_data_json_before: Array[String],
            session: Session, tep_Twos: DataFrame, json_topic_end: JSONObject): (String, String) = {
    val json_topic = JSON.parseObject(x._2)
    loggers.info("------接受到了kafka的数据------" + json_topic)
    val str = json_topic.getString("question") //体育,官,网,在,哪里
    val questionId = json_topic.getString("questionId")
    val category = json_topic.getString("category") //分类

    //过滤掉常用词汇,但也有全部过滤掉的风险，因此在这里我加了个判断
    val before_data_kafka = DicAnalysis.parse(str).recognition(stop).toStringWithOutNature(" ")
    val after_data_kafka = DicAnalysis.parse(str).recognition(stop).toStringWithOutNature(" ").split(" ").map(_.trim).filter(!filter_directory.contains(_))
    val result_str: String = if (after_data_kafka.length == 0) before_data_kafka else after_data_kafka.mkString(" ")
    //根据标签过滤
    val robot_data_json = robot_data_json_before.filter(x => {
      val category_and_default = JSON.parseObject(x).getString("category")
      if (category_and_default == category || category_and_default == "默认") true else false
    })

    val end_case = getProduct_result(robot_data_json, result_str, stop)
    //找到我数据集合中，举重最大的值
    val end_max = if (end_case.length < 1) 0.0 else end_case.map(x => ((x._1, x._2, x._3), x._4.toDouble)).reduce((x1, x2) => if (x1._2 > x2._2) x1 else x2)._2
    var json_data_topic = new JSONArray
    if (end_max < 0.5) {
      val graph_count = get_knowledge_graph(result_str, stop, session, category).length
      //举重小于0.5的时候发现知识图谱也没有，那么继续走知识库
      if (graph_count < 1) {
        json_data_topic = getAnswer(tep_Twos, result_str, stop, end_case)
        //走完知识库后发现也没有，再走一遍完全匹配
        if (json_data_topic.size() == 0) {
          json_data_topic = all_pipei(robot_data_json: Array[String], str: String)
          loggers.info(json_data_topic.toString + "---调用了完全匹配---")
        } else json_data_topic = getAnswer(tep_Twos, result_str, stop, end_case)
      } else {
        //将多个集合压扁，成一个集合
        val sum_list = get_knowledge_graph(result_str, stop, session, category).map(x => Seq(x._1, x._2, x._3))
        val one_list = sum_list.flatten.distinct.mkString(",")
        val result_end = DicAnalysis.parse(one_list).recognition(stop).toStringWithOutNature(" ")
        val tep_one = getProduct_result(robot_data_json: Array[String], result_str, stop)
        json_data_topic = getAnswer(tep_Twos, result_end, stop, tep_one)
      }
    }
    else json_data_topic = getAnswer(tep_Twos, result_str, stop, end_case)
    json_topic_end.put("questionId", questionId)
    json_topic_end.put("data", json_data_topic)
    loggers.info(s"打印数据,同时将数据输出到redis中------${json_topic_end.toJSONString}")
    val key = s"robot_res_$questionId"

    (key, json_topic_end.toJSONString)
  }

  def main(args: Array[String]): Unit = {

    val lines_source = Source.fromURL(getClass.getResource("/config-scala.properties")).getLines.toSeq
    org.apache.log4j.Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    //mysql配置
    val location_mysql_robot_url: String = lines_source(3).toString.split("==")(1)
    val location_mysql_url: String = lines_source(2).toString.split("==")(1)
    //redis配置
    val redisHost: String = lines_source(5).toString.split("==")(1)
    val redisPort: Int = lines_source(6).toString.split("==")(1).toInt
    //Neo4j配置
    val Neo4j_url: String = lines_source(7).toString.split("==")(1)
    val Neo4j_username: String = lines_source(8).toString.split("==")(1)
    val Neo4j_password: String = lines_source(9).toString.split("==")(1)
    val prop: Properties = new Properties
    //过滤标点符号
    val conf_s = new SparkConf().setAppName("robot_streaming").setMaster("local[4]")
    val sc = new SparkContext(conf_s)
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(1))
    val sqlContext: HiveContext = new HiveContext(sc)
    sqlContext.sql("set hive.exec.dynamic.partition.mode = nonstrict")
    val kafkaParam: Map[String, String] = Map[String, String](
      //-----------kafka低级api配置-----------
      "zookeeper.connect" -> "namenode2.cdh:2181,namenode1.cdh:2181", //----------配置zookeeper-----------
      "metadata.broker.list" -> "namenode1.cdh:9092",
      "group.id" -> "robot_data_mk", //设置一下group id
      "auto.offset.reset" -> kafka.api.OffsetRequest.LargestTimeString, //----------从该topic最新的位置开始读数------------
      "client.id" -> "robot_data_mk",
      "zookeeper.connection.timeout.ms" -> "10000"
    )
    val topicSet: Set[String] = Set("robot", "worktype_search")
    val directKafka: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topicSet)
    val lines: DStream[(String, String)] = directKafka.map((x: (String, String)) => (x._1, x._2))
    val df_Json: JSONObject = new JSONObject
    val df_Json_work_type: JSONObject = new JSONObject

    lines.foreachRDD(rdds => {
      if (!rdds.isEmpty) {
        //需要过滤的词典
        val tep_Twos: DataFrame = sqlContext.read.jdbc(location_mysql_robot_url, "robot_data", prop)
        val robot_data_json_before: Array[String] = tep_Twos.map(x => {
          x.schema.fieldNames.zip(x.toSeq.map(x => if (x == null || x == "") "" else x.toString)).map(x => df_Json.put(x._1, x._2))
          df_Json.toJSONString
        }).collect
        //标准工种
        val normal_classification_of_work: Array[String] = sqlContext.read.jdbc(location_mysql_url, "ods_policy_worktype_prd", prop).map(x => {
          x.schema.fieldNames.zip(x.toSeq.map(x => if (x == null || x == "") "" else x.toString)).map(x => df_Json_work_type.put(x._1, x._2))
          df_Json_work_type.toJSONString
        }).collect
        //对应工种表
        val ods_policy_worktype_map: Array[String] = sqlContext.read.jdbc(location_mysql_url, "ods_policy_worktype_map", prop).map(x => {
          x.schema.fieldNames.zip(x.toSeq.map(x => if (x == null || x == "") "" else x.toString)).map(x => df_Json_work_type.put(x._1, x._2))
          df_Json_work_type.toJSONString
        }).collect
        rdds.foreachPartition(x => {
          //常用词汇
          val filter_directory: Seq[String] = Source.fromURL(getClass.getResource("/diectory.txt")).getLines.toSeq
          val driver: Driver = GraphDatabase.driver(Neo4j_url, AuthTokens.basic(Neo4j_username, Neo4j_password))
          //  val driver: Driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "maokai"))
          val session: Session = driver.session
          val redisClient: Jedis = new Jedis(redisHost, redisPort)
          redisClient.auth("dmp@123$%^")
          redisClient.select(1)
          val loggers: log4j.Logger = org.apache.log4j.Logger.getLogger("logFile")
          val stop: StopRecognition = new StopRecognition //分词准备
          stop.insertStopNatures("w") //过滤掉标点
          val json_topic_end: JSONObject = new JSONObject
          val json_topic_end_work: JSONObject = new JSONObject
          x.foreach(x => {
            try {
              //走正常对话
              if (x._1.contains("robot")) {
                val str_robot = robot(stop: StopRecognition, x: (String, String), loggers: log4j.Logger, filter_directory: Seq[String], robot_data_json_before: Array[String],
                  session: Session, tep_Twos: DataFrame, json_topic_end: JSONObject)
                redisClient.setex(str_robot._1, 600, str_robot._2)
              }
              //走工种匹配
              else if (x._1.contains("worktype")) {
                val work_robot = robot_work_type(x: (String, String), loggers: log4j.Logger, stop: StopRecognition,
                  filter_directory: Seq[String], normal_classification_of_work: Array[String],
                  ods_policy_worktype_map: Array[String], json_topic_end_work: JSONObject)

                redisClient.setex(work_robot._1, 600, work_robot._2)
              }
            }
            catch {
              case e: Exception => loggers.error("数据计算报错------>原因:" + e.getMessage)
            }
          })
          loggers.info("------关闭redisClient客户端，与Neo4j客户端------")
          redisClient.close()
          session.close()
        })
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
