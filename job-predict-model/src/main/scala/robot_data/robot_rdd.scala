package robot_data

import java.util.Properties
import java.util.regex.Pattern

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.DicAnalysis
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.driver.v1._

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.control.Breaks._

/**
  * Created by MK on 2018/7/30
  * 中途有RDD
  */
object robot_rdd {

  //    val driver: Driver = GraphDatabase.driver("bolt://datanode2.cdh:7687", AuthTokens.basic("neo4j", "123456"))
  val driver: Driver = GraphDatabase.driver("bolt://namenode2.cdh:7687", AuthTokens.basic("neo4j", "123456"))
  //  val driver: Driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "maokai"))

  val session: Session = driver.session

  //分词准备
  val stop: StopRecognition = new StopRecognition
  stop.insertStopNatures("w") //过滤掉标点
  //    val category = "业务以及流程知识"


  //得到预测的结果案情
  def getProduct_result(tep_Twos: DataFrame, result_str: String, stop: StopRecognition): RDD[(String, Int, String, String)]
  = {
    val sentenceData =   tep_Twos.map(x => x.getAs[String]("query")).filter(x => {
      val p = Pattern.compile("[\u4e00-\u9fa5]")
      val m = p.matcher(x)
      if (m.find()) true else false
    })


//    val sentenceData = tep_Twos.map(x => x.getAs[String]("query"))


    //找到我历史数据中得值
    val res: Set[String] = sentenceData.flatMap(x => DicAnalysis.parse(x).recognition(stop).toStringWithOutNature(" ").split(" ")).collect().toSet

    //得到我这个案件描述中，都包含哪些个关键字
    val result: Array[String] = result_str.split(" ")
      .map(x => {
        val char_size = x.getBytes("UTF-8").length
        (x, char_size)
      }) //.filter(_._2 > 3)
      .map(_._1)
      .filter(res.contains)


    //找出我历史数据中出现的次数
    val result_value: RDD[(String, Int, String, String)] = tep_Twos.map(x=>{
      val query = x.getAs[String]("query")
      val answer = x.getAs[String]("answer")
      (query,answer)
    }).filter(x=>{
      val p = Pattern.compile("[\u4e00-\u9fa5]")
      val m = p.matcher(x._1)
      if (m.find()) true else false
    }) .map(x => {
      val query = x._1
      val answer = x._2
      val result_str = DicAnalysis.parse(query.replaceAll("\\d+", "")).recognition(stop).toStringWithOutNature(" ").split(" ")
      (query, result_str, answer)
    }).filter(line => line._2.exists(result.contains(_))).map(x => {
      val s = x._2.toSet & result.toSet //求得交集

      //x._2.toSet(知识库中每个问题，拆分完后出现了几个分词)
      //result.toSet(问的问题，拆分完后出现了几个分词)
      val person_query = result.toSet.size.toDouble //问的问题拆分完后的词的个数
      /**
        * 先计算出我问的问题，拆分完后与知识库中拆分完后的占比 再 乘以 （它们共有的分词个数/问的问题拆分完后的个数)
        **/
      val value_ratio = (person_query / x._2.toSet.size) * (s.size / person_query)
      (x._1, s.size, x._3, value_ratio.formatted("%.2f"))
    })
    val value_max = if (result_value.count() == 0) 0 else result_value.map(_._2).max
    //过滤出出现次数最多的值
    val end: RDD[(String, Int, String, String)] = result_value
      .filter(x => if (x._2 == value_max) true else false)


    end
  }

  //使用知识库并获得答案返回
  def getAnswer(tep_Twos: DataFrame, result_str: String, stop: StopRecognition, end_case: RDD[(String, Int, String, String)]): Unit
  = {
    //当解析得结果中有多条答案得话，随机选择一条，剩余的则是推荐
    val number = end_case.count
    if (number > 1) {
      val answer_first = end_case.first
      val n_show = answer_first._3.replace("mk", "\\\n").replace("\\", "") //换行显示
      println("答案占比:\r\n" + answer_first._1, answer_first._4)
      println("您好，根据您的提问，您所要了解的信息如下，如果不满意，下面还有后续的相关问题，希望能够帮到您:" + "\r\n" + n_show)
      //推荐的问答是
      println("推荐的问答是:")
      end_case.filter(x => x._1 != answer_first._1).map(x => (x._1, x._4)).take(5)

    } else if (number == 1) {
      val answer_first = end_case.first
      println("答案占比:" + answer_first._1, answer_first._4)
      val n_show = answer_first._3.replace("mk", "\\\n").replace("\\", "") //换行显示
      println("您好，根据您的提问，您所要了解的信息如下:" + "\r\n" + n_show)

    } else println("您好，根据您的提问，小牛无法提供答案呢...{{{(>_<)}}}")

  }

  //知识图谱
  /**
    * 查出所有节点名称和其对应的id,并与问题向匹配进而将多余的过滤掉
    **/
  def get_Node_ID(result_str: String, stop: StopRecognition): (Array[(String, String, String)], Array[String])
  = {
    val get_results = session.run("match(n) return  n.name as name,id(n) as id")
    val json = new JSONObject()
    val json_arr: JSONArray = new JSONArray

    while (get_results.hasNext) {
      val get_value = get_results.next()
      val node_name = get_value.get("name").asString()
      val id = get_value.get("id").asInt()
      json.put("id", id)
      json.put("node_name", node_name)
      json_arr.add(json.toJSONString)
      json.clear()
    }

    val get_Labels_Array = json_arr.toArray


    //将中文为一个字的过滤掉
    val result_str_two_chinese: Array[String] = result_str.split(" ").map(x => {
      val char_size = x.getBytes("UTF-8").length
      (x, char_size)
    }).filter(_._2 > 3).map(_._1)


    //找到我历史数据中得节点值
    val res: Array[(String, String, String)] = get_Labels_Array.map(x => {

      val json_b = JSON.parseObject(x.toString)
      val value = json_b.getString("node_name") //.recognition(stop).toStringWithOutNature(" ").split(" ")

      val value_list = DicAnalysis.parse(value).recognition(stop).toStringWithOutNature(" ").split(" ")

      (json_b.getString("node_name"), value, json_b.getString("id"), value_list)
    })
      .filter(line => line._4.exists(result_str_two_chinese.contains(_)))
      .map(x => (x._1, x._2, x._3))

    //      .filter(line => result_str_two_chinese.contains(line._2))
    res.foreach(println(_))


    (res, result_str_two_chinese)
  }

  //深层遍历知识图谱的时候，进行过滤节点
  def get_id(tep_one: Array[String]): Array[(String, String)] = {
    //得到所有的节点
    val get_results = session.run("match(n) return  n.name as name,id(n) as id")
    val json = new JSONObject()
    val json_arr: JSONArray = new JSONArray()

    while (get_results.hasNext) {
      val get_value = get_results.next()
      val node_name = get_value.get("name").asString()
      val id = get_value.get("id").asInt()
      json.put("id", id)
      json.put("node_name", node_name)
      json_arr.add(json.toJSONString)
      json.clear()
    }
    val end: Array[(String, String)] = json_arr.toArray().map(x => JSON.parseObject(x.toString))
      .map(x => (x.getString("node_name"), x.getString("id"))).filter(x => tep_one.contains(x._1))
    end
  }

  /**
    * 得到我每个节点的ID值，同时根据ID值找出与节点有关系的节点和关系名称
    **/
  def get_relationShip_name_id(res: Array[(String, String, String)], result_str: String): Array[(String, String, String)]
  = {
    //得到我每个节点的id值
    var get_node_id_value = res.map(_._3).mkString(",")
    var out_while = ""
    val json_arr_relation: JSONArray = new JSONArray()
    var replace_data = Array[String]()

    //循环迭代节点数据
    def get_while(relation_node: StatementResult, relation_ships_and_node: JSONObject): Unit = {
      while (relation_node.hasNext) {
        val tep = relation_node.next()
        val relation_ships_node_type = tep.get("relation_ships_node_type").asString
        val relation_ships_node_name = tep.get("relation_ships_node_name").asString
        val local_name = tep.get("local_name").asString
        relation_ships_and_node.put("relation_ships_node_type", relation_ships_node_type)
        relation_ships_and_node.put("relation_ships_node_name", relation_ships_node_name)
        relation_ships_and_node.put("local_name", local_name)
        json_arr_relation.add(relation_ships_and_node.toJSONString)
        relation_ships_and_node.clear()
      }
    }


    //迭代，知识图谱
    breakable {
      for (a <- 1 to 10) { //1，2，3，4
        if (a == 8 || out_while == "out") break; //当a等于8时跳出breakable块，也就是说我只循环了7次
        //得到与其有关系的节点名称，和2者之间的关系名称
        val relation_node: StatementResult = session.run(s"unwind[$get_node_id_value] as line match(n)-[r] ->(m) where id(n)=line  return  n.name as local_name , type(r) as relation_ships_node_type ,m.name as relation_ships_node_name")
        val relation_ships_and_node = new JSONObject()

        /**
          * one、two、three
          **/
        if (res.length == 1) {
          //one:单节点迭代
          get_while(relation_node, relation_ships_and_node)
          replace_data = json_arr_relation.toArray.distinct.map(_.toString)
          out_while = "out"
        } else {
          //two:多节点深层迭代(8层)
          get_while(relation_node: StatementResult, relation_ships_and_node: JSONObject) //多节点的话深层迭代
          val tep_one: Array[String] = json_arr_relation.toArray.map(x => JSON.parseObject(x.toString)).map(x => x.getString("relation_ships_node_name"))
          get_node_id_value = get_id(tep_one).map(_._2).mkString(",")
          //println("循环第" + a + "次------------------")
          val sa = json_arr_relation.toArray().distinct.map(x => (JSON.parseObject(x.toString).getString("relation_ships_node_name"), x.toString))
          if (sa.map(_._1).toSet.size != sa.length) out_while = "out"
          //有重复,退出循环,同时将重复的过滤出来
          val end_Hehe: Array[String] = sa.groupBy(_._1).filter(x => x._2.length > 1).flatMap(x => x._2.map(_._2))(collection.breakOut) //将重复的过滤出来
          replace_data = end_Hehe

          //three:如果我输入有多个节点的话，且多个节点最终没有相交的节点，那么我就取得第一次指向得节点
          if (a == 7) {
            json_arr_relation.clear()
            val relation_ships_and_node_new = new JSONObject()
            val relation_node_one = session.run(s"unwind[${res.map(_._3).mkString(",")}] as line match(n)-[r] ->(m) where id(n)=line  return  n.name as local_name , type(r) as relation_ships_node_type ,m.name as relation_ships_node_name")
            get_while(relation_node_one, relation_ships_and_node_new)
            replace_data = json_arr_relation.toArray.distinct.map(_.toString)
            out_while = "out"
          }
        }
      }
    }
    val end: Array[(String, String, String)] = replace_data.map(x => {
      val json_b = JSON.parseObject(x)
      (json_b.getString("relation_ships_node_type"), json_b.getString("relation_ships_node_name"), json_b.getString("local_name"))
    })

    end
  }

  /**
    * 通过我的问题，从而找到我的关系，将知识图谱中的关系，过滤出来
    **/
  def filter_relationShips(stop: StopRecognition, result_str_two_chinese: Array[String], last_one: Array[(String, String, String)]): Array[(String, String, String)]
  = {
    val relation_ships_name_arr = ArrayBuffer[String]()
    //得到我Neo4j中的所有关系名称
    val relation_ships_res = session.run("match(n)-[r]-() return type(r) as relationShips")
    while (relation_ships_res.hasNext) {
      val relation_ships_name = relation_ships_res.next().get("relationShips").asString()
      relation_ships_name_arr += relation_ships_name
    }
    //将我的问题中的关系过滤出来
    val relaction_filter = relation_ships_name_arr.distinct.map(x => {
      val value = DicAnalysis.parse(x).recognition(stop).toStringWithOutNature(" ").split(" ")
      (x, value)
    }).filter(line => line._2.exists(result_str_two_chinese.contains(_))).map(_._1)

    //因为问题中没有关系，所以就没有值，因为我下面用到了，过滤
    //过滤找出与问题有关的：关系和节点
    val end: Array[(String, String, String)] = last_one.filter(x => relaction_filter.contains(x._1)).distinct
    end
  }

  /**
    * 调用知识图谱
    **/
  def get_knowledge_graph(result_str: String, stop: StopRecognition): Array[(String, String, String)]
  = {
    //1、查出所有节点名称和其对应的id,并与问题向匹配进而将多余的过滤掉
    val res: Array[(String, String, String)] = get_Node_ID(result_str: String, stop: StopRecognition)._1

    val result_str_two_chinese: Array[String] = get_Node_ID(result_str: String, stop: StopRecognition)._2

    //2、得到我每个节点的ID值，同时根据ID值找出与节点有关系的节点和关系名称
    val last_one: Array[(String, String, String)] = get_relationShip_name_id(res, result_str)

    //3、通过我的问题，从而找到我的关系，将知识图谱中的关系，过滤出来
    //    val end: Array[(String, String, String)] = filter_relationShips(stop, result_str_two_chinese, last_one)
    //    end

    //    不过滤关系了
    last_one
  }

  def main(args: Array[String]): Unit = {
    val filter_directory = Source.fromURL(getClass.getResource("/diectory.txt")).getLines.toSeq
    val str = "校长" //体育,官,网,在,哪里

    val before_data_kafka = DicAnalysis.parse(str).recognition(stop).toStringWithOutNature(" ")
    val after_data_kafka = DicAnalysis.parse(str).recognition(stop).toStringWithOutNature(" ").split(" ").map(_.trim).filter(!filter_directory.contains(_))
    val result_str: String = if (after_data_kafka.length == 0) before_data_kafka else after_data_kafka.mkString(" ")

    //    val result_str: String = DicAnalysis.parse(str).recognition(stop).toStringWithOutNature(" ")
    println(result_str.split(" ").mkString(","))
    //过滤标点符号
    val conf_s = new SparkConf().setAppName("wuYu").setMaster("local[4]")
    val sc = new SparkContext(conf_s)
    val sqlContext: HiveContext = new HiveContext(sc)
    val prop: Properties = new Properties
    //    val tep_Twos = sqlContext.sql("SELECT * from robot.robot_data").where(s"category!='工伤基础知识' and category not like '%业务%' and category!='保险基础知识' and category!='业务以及流程知识'")
    val tep_Twos = sqlContext.read.jdbc("jdbc:mysql://172.16.11.105:3306/robotdb?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&user=root&password=bzn@cdh123!", "robot_data", prop).where(s"category!='工伤基础知识' and category not like '%业务%' and category!='保险基础知识' and category!='业务以及流程知识'")
      .cache

    val end_case: RDD[(String, Int, String, String)] = getProduct_result(tep_Twos, result_str, stop)
    //找到我数据集合中，举重最大的值
    val end_max = if (end_case.count() < 1) 0.0 else end_case.map(x => ((x._1, x._2, x._3), x._4.toDouble)).reduce((x1, x2) => if (x1._2 > x2._2) x1 else x2)._2

    if (end_max < 0.5) {
      val graph_count = get_knowledge_graph(result_str, stop).length
      //举重小于0.5的时候发现知识图谱也没有，那么继续走知识库
      if (graph_count < 1) {
        println("进入了这里=======")
        println(result_str)
        getAnswer(tep_Twos, result_str, stop, end_case)
      } else {
        println("走这里了吗？")
        //将多个集合压扁，成一个集合
        val sum_list = get_knowledge_graph(result_str, stop).map(x => Seq(x._1, x._2, x._3))
        val one_list = sum_list.flatten.distinct.mkString(",")
        val result_end = DicAnalysis.parse(one_list).recognition(stop).toStringWithOutNature(" ")
        val tep_one = getProduct_result(tep_Twos, result_end, stop)
        getAnswer(tep_Twos, result_end, stop, tep_one)
        println("老铁666节点老铁666节点老铁666节点老铁666节点老铁666节点老铁666节点老铁666节点老铁666节点老铁666节点老铁666节点老铁666节点老铁666节点")
        get_knowledge_graph(result_str, stop)
      }
    }
    else {
      println("走了吗？")
      getAnswer(tep_Twos, result_str, stop, end_case)
    }
  }
}
