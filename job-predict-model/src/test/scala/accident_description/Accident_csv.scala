package accident_description

import java.io.StringReader
import java.util.Properties
import java.util.regex.Pattern

import au.com.bytecode.opencsv.CSVReader
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.DicAnalysis
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MK on 2018/6/6.
  */
object Accident_csv {
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
  def filter_word(sentenceData: RDD[String], stop: StopRecognition): Set[String]
  = {
    val end: Set[String] = sentenceData.flatMap(x => DicAnalysis.parse(x).recognition(stop).toStringWithOutNature(" ").split(" "))
      .map((_, 1)).reduceByKey(_ + _).filter(x => if (x._1.length == 2 && x._2 >= 10) true else false)
      .sortBy(_._2, ascending = false)
      .map(x => (judgeContainsStr(x._1), x._1, x._2)).filter(x => if (!x._1._1 && !x._1._2) true else false)
      .map(x => x._2).collect().toSet
    end
  }

  //读取mysql与hive的数据
  def read_mysql(sqlContext: HiveContext): (DataFrame, RDD[(String, String, String)])
  = {
    val ods_policy_product_plan: DataFrame = sqlContext.sql("select * from odsdb_prd.ods_policy_product_plan").cache

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
  def getProduct_result(tep_Twos: DataFrame, result_str: String, stop: StopRecognition): RDD[(String, Int, String, String)]
  = {
    val sentenceData = tep_Twos.select("案情").map(_.toString)
    val res = filter_word(sentenceData, stop)
    //得到我这个案件描述中，都包含哪些个关键字
    val result = result_str.split(" ").filter(res.contains)
    //找出我历史数据中出现的次数
    val result_value: RDD[(String, Int, String, String)] = tep_Twos.filter("预估赔付金额!=0").map(x => {
      val money = x.getAs[String]("预估赔付金额")
      val value = x.getAs[String]("案情")
      val policy_id = x.getAs[String]("保单号（禁止输入换行符和空格）")
      val result_str = DicAnalysis.parse(value.replaceAll("\\d+", "")).recognition(stop).toStringWithOutNature(" ").split(" ")
      (value, result_str, money, policy_id)
    }).filter(line => line._2.exists(result.contains(_))).map(x => {
      val s = x._2.toSet & result.toSet
      (x._1, s.size, x._3, x._4)
    })
    val value_max = if (result_value.count() == 0) 0 else result_value.map(_._2).max

    //过滤出出现次数最多的值
    val end = result_value.filter(x => if (x._2 == value_max || x._2 == value_max - 1) true else false)
    end
  }


  //计算数据出现的次数，以及赔付金额的总额
  def get_numver_count_case(end_case: RDD[(String, Int, String, String)]): (Int, Int)
  = {
    val res_num = if (end_case.count() == 0) (0, 1) else {
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
  def data_Validation(res_num: (Int, Int), end_medical: RDD[(String, String, String)], policy_id: String): Unit = {
    //通过算法预测的金额
    val forecast_Amount = res_num._1 / res_num._2
    //实际不应超过的赔付金额
    val end_medical_arr = end_medical.map(_._1).collect
    //判断我要找的该保单ID在数据库中是否存在如果不存在的话，就用预测的
    val result = if (end_medical_arr.contains(policy_id)) {
      val end = end_medical.filter(x => if (x._2.contains("医疗") && x._1 == policy_id) true else false).first._3.toInt
      val result = if (forecast_Amount <= end) forecast_Amount else end
      result
    } else forecast_Amount
    println("结果", result)

  }

  def main(args: Array[String]): Unit = {
    //分词准备
    val stop: StopRecognition = new StopRecognition
    stop.insertStopNatures("w") //过滤掉标点

    val str = "  6.14下午8点左右事故发生地点：淄博市博山区源泉镇岳东村盛杰玻璃厂附近事故描述：下夜班在盛杰玻璃厂附近与另一辆摩托车相撞/现在重症监护室暂时未清醒/其他细节后续补充！]HL1100000029006368"
    val result_str: String = DicAnalysis.parse(str).recognition(stop).toStringWithOutNature(" ")

    //过滤标点符号
    val str_tep_one = str.split("]")(0)
    val str_tep_two = str_tep_one.replaceAll("[\\pP‘’“”]", "").replace(" ", "")
    println(str_tep_two)

    //得到保单号
    //    val policy_id_one = str.split("]")(1)
    //案情
    //    val anjian = str.split("]")(0)

    val conf_s = new SparkConf().setAppName("wuYu").setMaster("local[2]")
    val sc = new SparkContext(conf_s)
    val sqlContext: HiveContext = new HiveContext(sc)
    val path_big = "C:\\Users\\a2589\\Desktop\\需求one\\案情.csv"
    import sqlContext.implicits._
    val tep_Twos: DataFrame = big_table(sc, sqlContext, path_big) // .where(s"'保单号（禁止输入换行符和空格）' != '$policy_id_one'")
      //过滤历史数据中相同的保单号
      .map(x => {
      val case_information = x.getAs[String]("案情")
      val pre_payment = x.getAs[String]("预估赔付金额")
      val policy_id = x.getAs[String]("保单号（禁止输入换行符和空格）")
      (case_information, pre_payment, policy_id)
    }).filter(_._1.replaceAll("[\\pP‘’“”]", "").replace(" ", "") != str_tep_two).toDF("案情", "预估赔付金额", "保单号（禁止输入换行符和空格）")
    tep_Twos.printSchema
    //    要想select有效必须保证该列是printSchema打印出来的
    //    tep_Twos.select("案情")

    //读取mysql与hive的数据
    val ods_policy_product_plan = read_mysql(sqlContext: HiveContext)._1
    val end_medical: RDD[(String, String, String)] = read_mysql(sqlContext: HiveContext)._2


    //得到我输入案情的保单号
    val policy_id = str.split("]")(1)
    if (str.contains("死亡")) {
      //如果该案件是死亡案件，则到相对应的表中，查询起对应的赔付金额,通过保单ID查询死亡金额
      ods_policy_product_plan.map(x => (x.getAs[String]("policy_code"), x.getAs[String]("sku_coverage") + "0000"))
        .filter(_._1 == policy_id)
        .foreach(x => println("结果", x))
    } else {
      if (str.contains("骨折") || str.contains("残疾")) {
        //得到预测的结果案情
        val end_case = getProduct_result(tep_Twos: DataFrame, result_str: String, stop: StopRecognition).filter(x => if (x._1.contains("残疾") || x._1.contains("骨折")) true else false)
        println("执行的我1")
        end_case.foreach(println(_))
        //计算数据出现的次数，以及赔付金额的总额
        val res_num: (Int, Int) = get_numver_count_case(end_case: RDD[(String, Int, String, String)])
        //调用算法逻辑
        data_Validation(res_num: (Int, Int), end_medical: RDD[(String, String, String)], policy_id: String)
      } else if (str.contains("手术")) {
        //得到预测的结果案情
        val end_case_two: RDD[(String, Int, String, String)] = getProduct_result(tep_Twos: DataFrame, result_str: String, stop: StopRecognition)
        val end_case_three = end_case_two.filter(x => if (x._1.contains("手术")) true else false) // filter(!_._1.contains("死亡")).filter(x => if (!x._1.contains("骨折") || !x._1.contains("残疾")) true else false)
        println("执行的我2")
        end_case_three.foreach(println(_))
        //计算数据出现的次数，以及赔付金额的总额
        val res_num: (Int, Int) = get_numver_count_case(end_case_three: RDD[(String, Int, String, String)])
        //调用算法逻辑
        data_Validation(res_num: (Int, Int), end_medical: RDD[(String, String, String)], policy_id: String)
      } else {
        //得到预测的结果案情
        val end_case_two: RDD[(String, Int, String, String)] = getProduct_result(tep_Twos: DataFrame, result_str: String, stop: StopRecognition)
        val end_case_three = end_case_two.filter(x => if (x._1.contains("死亡") || x._1.contains("残疾") || x._1.contains("骨折") || x._1.contains("伤残") || x._1.contains("手术")) false else true) // filter(!_._1.contains("死亡")).filter(x => if (!x._1.contains("骨折") || !x._1.contains("残疾")) true else false)
        println("执行的我3")
        end_case_three.foreach(println(_))
        //计算数据出现的次数，以及赔付金额的总额
        val res_num: (Int, Int) = get_numver_count_case(end_case_three: RDD[(String, Int, String, String)])
        //调用算法逻辑
        data_Validation(res_num: (Int, Int), end_medical: RDD[(String, String, String)], policy_id: String)
      }
    }
    sc.stop()
  }


}
