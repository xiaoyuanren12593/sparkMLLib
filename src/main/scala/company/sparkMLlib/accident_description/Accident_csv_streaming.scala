package company.sparkMLlib.accident_description

import java.io.{FileWriter, StringReader}
import java.util.Properties
import java.util.regex.Pattern

import au.com.bytecode.opencsv.CSVReader
import kafka.serializer.StringDecoder
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.DicAnalysis
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MK on 2018/6/6.
  */
object Accident_csv_streaming {
  //大表
  def big_table(sc: SparkContext, sQLContext: SQLContext, path_big: String): DataFrame
  = {
    //雇主大表
    val big_tepOne = sc.textFile(path_big).filter(_ != null).map(x => {
      val reader = new CSVReader(new StringReader(x))
      reader.readNext
    }).filter(x => x != null && x.length == 20)

    //取得列名,并将其作为字段名
    val schema_tepOne = StructType(big_tepOne.first.map(fieldName => StructField(fieldName, StringType, nullable = true)))
    //字段对应的值
    val value_tepTwo = big_tepOne.filter(_ != null).map(r => Row(r: _*))
    //大表生成DF
    val big = sQLContext.createDataFrame(value_tepTwo, schema_tepOne)
    big
  }

  def judgeContainsStr(cardNum: String): (Boolean, Boolean)
  = {
    //是否包含字母
    val regex = ".*[a-zA-Z]+.*"
    val m = Pattern.compile(regex).matcher(cardNum)

    //是否包含数字
    val number_regex = ".*\\d+.*"
    val number = Pattern.compile(number_regex).matcher(cardNum)
    (m.matches, number.matches())
  }

  //找到我历史数据中的词语
  def filter_word(sentenceData: Array[String], stop: StopRecognition): Set[String]
  = {
    val end = sentenceData.filter(_.length > 0).flatMap(x => DicAnalysis.parse(x).recognition(stop).toStringWithOutNature(" ").split(" "))
      .map((_, 1)).groupBy(_._1).map(x => {
      (x._1, x._2.length)
    }).filter(x => if (x._1.length == 2 && x._2 >= 10) true else false)
      .map(x => (judgeContainsStr(x._1), x._1, x._2)).filter(x => if (!x._1._1 && !x._1._2) true else false)
      .map(x => x._2).toSet
    end
  }

  //读取mysql与hive的数据
  def read_mysql(sqlContext: HiveContext): (DataFrame, RDD[(String, String, String)])
  = {
    val ods_policy_product_plan = sqlContext.sql("select * from odsdb_prd.ods_policy_product_plan").cache

    val url = "jdbc:mysql://172.16.11.105:3306/odsdb?user=odsuser&password=odsuser"

    val prop = new Properties()
    val b_policy = sqlContext.read.jdbc(url, "b_policy", prop).cache
    val b_policy_product_plan_item = sqlContext.read.jdbc(url, "b_policy_product_plan_item", prop).cache
    val medical_Care = b_policy.join(b_policy_product_plan_item, "policy_no").select("insurance_policy_no", "item_name", "item_amount")

    val end_medical = medical_Care.map(x => {
      val insurance_policy_no = x.getAs[String]("insurance_policy_no")
      val item_name = x.getAs[String]("item_name")
      val item_amount = x.getAs[String]("item_amount")
      (insurance_policy_no, item_name, item_amount)
    }).cache
    (ods_policy_product_plan, end_medical)

  }


  //得到预测的结果案情
  def getProduct_result(tep_Twos: Array[Row], result_str: String, stop: StopRecognition): Array[(String, Int, String, String)]
  = {
    val sentenceData: Array[String] = tep_Twos.map(_.getAs[String]("案情"))
    val res = filter_word(sentenceData, stop)
    //得到我这个案件描述中，都包含哪些个关键字
    val result = result_str.split(" ").filter(res.contains)
    //找出我历史数据中出现的次数
    val result_value = tep_Twos.filter(_.length > 0).filter(x => {
      if (x.getAs[String]("预估赔付金额").length > 0 && x.getAs[String]("案情").length > 0) true else false
    }).map(x => {
      val money = x.getAs[String]("预估赔付金额")
      val value = x.getAs[String]("案情")
      val policy_id = x.getAs[String]("保单号（禁止输入换行符和空格）")
      val result_str = DicAnalysis.parse(value.replaceAll("\\d+", "")).recognition(stop).toStringWithOutNature(" ").split(" ")
      (value, result_str, money, policy_id)
    }).filter(line => line._2.exists(result.contains(_))).map(x => {
      val s = x._2.toSet & result.toSet
      (x._1, s.size, x._3, x._4)
    })
    val value_max = if (result_value.length == 0) 0 else result_value.map(_._2).max
    //      val value_max = result_value.map(_._2).max
    //过滤出出现次数最多的值
    val end: Array[(String, Int, String, String)] = result_value.filter(x => if (x._2 == value_max || x._2 == value_max - 1) true else false)
    end
  }


  //计算数据出现的次数，以及赔付金额的总额
  def get_numver_count_case(end_case: Array[(String, Int, String, String)]): (Int, Int)
  = {
    val res_num = if (end_case.length == 0) (0, 1) else {
      val end: (Int, Int) = end_case.map(x => (x._3.toInt, 1)).reduce((x1, x2) => {
        val sum = x1._1 + x2._1
        val number = x1._2 + x2._2
        (sum, number)
      })
      end
    }
    res_num
  }

  //将预测的结果与mysql和hive数据中保单号的验证，同时进行计算
  def data_Validation(res_num: (Int, Int), policy_id: String, end_medical_arry: Array[(String, String, String)]): Double = {
    //通过算法预测的金额
    val forecast_Amount = res_num._1 / res_num._2
    //实际不应超过的赔付金额
    val end_medical_arr = end_medical_arry.map(x => x._1)
    //判断我要找的该保单ID在数据库中是否存在如果不存在的话，就用预测的
    val result = if (end_medical_arr.contains(policy_id)) {
      val end = end_medical_arry.filter(x => if (x._2.contains("医疗") && x._1 == policy_id) true else false).head._3.toDouble
      val result = if (forecast_Amount <= end) forecast_Amount else end
      result
    } else forecast_Amount
    result
  }

  def main(args: Array[String]): Unit = {
    val conf_s = new SparkConf().setAppName("wuYu").setMaster("local[2]")
    val sc = new SparkContext(conf_s)
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(2))

    val sqlContext: HiveContext = new HiveContext(sc)
    import sqlContext.implicits._
    val path_big = "C:\\Users\\a2589\\Desktop\\需求one\\案情.csv"
    val tep_Twos_one: Array[Row] = big_table(sc, sqlContext, path_big)
      .map(x => {
        val case_information = x.getAs[String]("案情")
        val pre_payment = x.getAs[String]("预估赔付金额")
        val policy_id = x.getAs[String]("保单号（禁止输入换行符和空格）")
        val number_code = x.getAs[String]("\uFEFF序号")
        (case_information, pre_payment, policy_id, number_code)
      }).toDF("案情", "预估赔付金额", "保单号（禁止输入换行符和空格）", "序号")
      .collect


    /**
      * kafka conf
      **/
    val kafkaParam: Map[String, String] = Map[String, String](
      //-----------kafka低级api配置-----------
      "zookeeper.connect" -> "namenode2.cdh:2181,datanode3.cdh:2181,namenode1.cdh:2181", //----------配置zookeeper-----------
      "metadata.broker.list" -> "namenode1.cdh:9092",
      "group.id" -> "spark_MLlib", //设置一下group id
      "auto.offset.reset" -> kafka.api.OffsetRequest.LargestTimeString, //----------从该topic最新的位置开始读数------------
      "client.id" -> "spark_MLlib",
      "zookeeper.connection.timeout.ms" -> "10000"
    )
    val topicSet: Set[String] = Set("test-kevin")
    val directKafka: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topicSet)
    val lines: DStream[(String, String)] = directKafka.map((x: (String, String)) => (x._1, x._2))
    // kafka取出的数据，_1是其topic，_2是消息

    //分词准备
    val stop: StopRecognition = new StopRecognition
    stop.insertStopNatures("w") //过滤掉标点
    //读取mysql与hive的数据
    val ods_policy_product_plan = read_mysql(sqlContext: HiveContext)._1.cache.collect
    val end_medical: RDD[(String, String, String)] = read_mysql(sqlContext: HiveContext)._2
    val end_medical_arry: Array[(String, String, String)] = end_medical.collect()


    val number = lines.count()
    println(number)
    lines.foreachRDD(rdd => {
      if (!rdd.isEmpty()) rdd.map(x => {
        val str = x._2
        val result_str = DicAnalysis.parse(str).recognition(stop).toStringWithOutNature(" ")
        //得到我输入案情的保单号
        val policy_id = x._2.split("]")(2)
        //案情
        val anqing = x._2.split("]")(0).replaceAll("[\\pP‘’“”]", "").replace(" ", "")

        //序号
        val number_code = x._2.split("]")(3)

        //将历史数据完全相等的保单号过滤掉
        val tep_Twos = tep_Twos_one.filter(x => {
          val policy_id_res = x.getAs[String]("保单号（禁止输入换行符和空格）")
          val anjian = x.getAs[String]("案情").replaceAll("[\\pP‘’“”]", "").replace(" ", "")
          //序号
          val number_code_history = x.getAs[String]("序号")

          //          if (policy_id_res != policy_id) true else false
          //          if (anjian != anqing) true else false
          if (number_code != number_code_history) true else false
        })

        val end = if (str.contains("死亡")) {
          //如果该案件是死亡案件，则到相对应的表中，查询起对应的赔付金额
          val end_death = ods_policy_product_plan.map(x => (x.getAs[String]("policy_code"), x.getAs[String]("sku_coverage") + "0000")).filter(_._1 == policy_id)
          val end = if (end_death.length > 0) end_death.head._2.toInt else "死亡案情无法通过保单号进行历史数据查找"
          end
        } else {
          val tep_one = if (str.contains("骨折") || str.contains("残疾")) {
            //得到预测的结果案情
            val end_case = getProduct_result(tep_Twos: Array[Row], result_str: String, stop: StopRecognition).filter(x => if (x._1.contains("残疾") || x._1.contains("骨折")) true else false)
            //                        println(s"执行的我1:$str")
            //                        end_case.foreach(x => println(x))
            //计算数据出现的次数，以及赔付金额的总额
            val res_num: (Int, Int) = get_numver_count_case(end_case: Array[(String, Int, String, String)])
            //调用算法逻辑
            val tep_two: Double = data_Validation(res_num: (Int, Int), policy_id: String, end_medical_arry)
            tep_two
          } else if (str.contains("手术")) {
            val end_case_two = getProduct_result(tep_Twos: Array[Row], result_str: String, stop: StopRecognition)
            val end_case_three = end_case_two.filter(x => if (x._1.contains("手术")) true else false) // filter(!_._1.contains("死亡")).filter(x => if (!x._1.contains("骨折") || !x._1.contains("残疾")) true else false)
            //            println(s"执行的我2:$str")
            //            end_case_three.foreach(println(_))

            //计算数据出现的次数，以及赔付金额的总额
            val res_num: (Int, Int) = get_numver_count_case(end_case_three: Array[(String, Int, String, String)])
            //调用算法逻辑
            val tep_three: Double = data_Validation(res_num: (Int, Int), policy_id: String, end_medical_arry)
            tep_three
          } else {
            //得到预测的结果案情
            val end_case_two = getProduct_result(tep_Twos: Array[Row], result_str: String, stop: StopRecognition)
            val end_case_three = end_case_two.filter(x => if (x._1.contains("死亡") || x._1.contains("残疾") || x._1.contains("骨折") || x._1.contains("伤残") || x._1.contains("手术")) false else true) // filter(!_._1.contains("死亡")).filter(x => if (!x._1.contains("骨折") || !x._1.contains("残疾")) true else false)
            //            println(s"执行的我3:$str")
            //            end_case_three.foreach(println(_))

            //计算数据出现的次数，以及赔付金额的总额
            val res_num: (Int, Int) = get_numver_count_case(end_case_three: Array[(String, Int, String, String)])
            //调用算法逻辑
            val tep_three: Double = data_Validation(res_num: (Int, Int), policy_id: String, end_medical_arry)
            tep_three
          }
          tep_one
        }
        val tep_fianlly = if (end == 0) (4000, str) else (end, str)
        tep_fianlly
      }).foreachPartition(rdds => {
        val out = new FileWriter("C:\\Users\\a2589\\Desktop\\w.txt", true) //false重新写,true不重新写
        rdds.foreach(x => {
          out.write(s"\r\n预测结果: ${x._1}, 原数据: ${x._2}")
          out.flush()
        })
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
