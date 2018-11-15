package company.robot_data.robot_until_base

import java.util.regex.Pattern

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.DicAnalysis
import org.apache.spark.sql.DataFrame
import org.neo4j.driver.v1.{Session, StatementResult}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

/**
  * Created by MK on 2018/10/9.
  * 基础函数处理
  */
trait until {
  //得到预测的结果案情
  def getProduct_result(robot_data_json_before: Array[String], result_str: String, stop: StopRecognition): Array[(String, Int, String, String, Integer, String, String, String, String, String, String, String, String)]
  = {
    //鉴定是否为空
    def if_not_null(str: String): String = {
      if (str == null || str == "null" || str == "") "" else str
    }

    val robot_data_json = robot_data_json_before.filter(x => {
      val query = JSON.parseObject(x).getString("query")
      val p = Pattern.compile("[\u4e00-\u9fa5]")
      val m = p.matcher(query)
      if (m.find()) true else false
    })


    val sentenceData = robot_data_json.map(x => JSON.parseObject(x).getString("query")) //找到我历史数据中得值
    val res: Set[String] = sentenceData.flatMap(x => DicAnalysis.parse(x).recognition(stop).toStringWithOutNature(" ").split(" ")).toSet

    //得到我这个案件描述中，都包含哪些个关键字
    val result: Array[String] = result_str.split(" ")
      .map(x => {
        val char_size = x.getBytes("UTF-8").length
        (x, char_size)
      }) //.filter(_._2 > 3)
      .map(_._1)
      .filter(res.contains)


    //找出我历史数据中出现的次数
    val result_value = robot_data_json.map(x => {
      val json_res = JSON.parseObject(x)
      val query = json_res.getString("query")
      val answer = json_res.getString("answer")
      val result_str = DicAnalysis.parse(query.replaceAll("\\d+", "")).recognition(stop).toStringWithOutNature(" ").split(" ")
      (query, result_str, answer, x)
    }).filter(line => line._2.exists(result.contains(_))).map(x => {
      val json_res = JSON.parseObject(x._4)
      val id = json_res.getInteger("id")
      val summary = json_res.getString("summary")
      val type_res = json_res.getString("type")
      val keywords = json_res.getString("keywords")

      val title = if_not_null(json_res.getString("title"))
      val introduce = if_not_null(json_res.getString("introduce"))
      val pic_url = if_not_null(json_res.getString("pic_url"))
      val link_url = if_not_null(json_res.getString("link_url"))
      val pic_type = if_not_null(json_res.getString("pic_type"))


      val s = x._2.toSet & result.toSet //求得交集

      //x._2.toSet(知识库中每个问题，拆分完后出现了几个分词)
      //result.toSet(问的问题，拆分完后出现了几个分词)
      val person_query = result.toSet.size.toDouble //问的问题拆分完后的词的个数
      /**
        * 先计算出我问的问题，拆分完后与知识库中拆分完后的占比 再 乘以 （它们共有的分词个数/问的问题拆分完后的个数)
        **/
      val value_ratio = (person_query / x._2.toSet.size) * (s.size / person_query)
      (x._1, s.size, x._3, value_ratio.formatted("%.2f"), id, summary, type_res, keywords, title, introduce, pic_url, link_url, pic_type)
    })
    val value_max = if (result_value.length == 0) 0 else result_value.map(_._2).max
    //过滤出出现次数最多的值
    val end: Array[(String, Int, String, String, Integer, String, String, String, String, String, String, String, String)] = result_value.filter(x => if (x._2 == value_max) true else false)
    end
  }

  //使用知识库并获得答案返回
  def getAnswer(tep_Twos: DataFrame, result_str: String, stop: StopRecognition, end_case: Array[(String, Int, String, String, Integer, String, String, String, String, String, String, String, String)]): JSONArray
  = {
    val response_Json_array: JSONArray = new JSONArray()
    //排序:只取得前6个
    val result_order = end_case.sortBy(x => x._4.toDouble).reverse.take(6) //foreach(println(_))
    for (i <- result_order.indices) {
      val response_Json = new JSONObject()
      response_Json.put("query", result_order(i)._1)
      response_Json.put("answer", result_order(i)._3)
      response_Json.put("id", result_order(i)._5)
      response_Json.put("summary", result_order(i)._6)
      response_Json.put("type", result_order(i)._7)
      response_Json.put("keywords", result_order(i)._8)
      response_Json.put("level", i + 1) //等级:1是最优先的

      response_Json.put("title", result_order(i)._9)
      response_Json.put("introduce", result_order(i)._10)
      response_Json.put("pic_url", result_order(i)._11)
      response_Json.put("link_url", result_order(i)._12)
      response_Json.put("pic_type", result_order(i)._13)


      response_Json_array.add(response_Json)
    }
    response_Json_array

  }

  //知识图谱
  /**
    * 查出所有节点名称和其对应的id,并与问题向匹配进而将多余的过滤掉
    **/
  def get_Node_ID(result_str: String, stop: StopRecognition, session: Session, category: String): (Array[(String, String, String)], Array[String])
  = {
    val get_results = session.run(s"match(n:$category) return  n.name as name,id(n) as id")
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
    (res, result_str_two_chinese)
  }

  //深层遍历知识图谱的时候，进行过滤节点
  def get_id(tep_one: Array[String], session: Session): Array[(String, String)]
  = {
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
  def get_relationShip_name_id(res: Array[(String, String, String)], result_str: String, session: Session): Array[(String, String, String)]
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
          get_node_id_value = get_id(tep_one, session).map(_._2).mkString(",")
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
  def filter_relationShips(stop: StopRecognition, result_str_two_chinese: Array[String], last_one: Array[(String, String, String)], session: Session): Array[(String, String, String)]
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
  def get_knowledge_graph(result_str: String, stop: StopRecognition, session: Session, category: String): Array[(String, String, String)]
  = {
    //1、查出所有节点名称和其对应的id,并与问题向匹配进而将多余的过滤掉
    val res: Array[(String, String, String)] = get_Node_ID(result_str, stop: StopRecognition, session, category)._1

    //        val result_str_two_chinese: Array[String] = get_Node_ID(result_str, stop: StopRecognition,session)._2

    //2、得到我每个节点的ID值，同时根据ID值找出与节点有关系的节点和关系名称
    val last_one: Array[(String, String, String)] = get_relationShip_name_id(res, result_str, session)

    //3、通过我的问题，从而找到我的关系，将知识图谱中的关系，过滤出来
    //    val end: Array[(String, String, String)] = filter_relationShips(stop, result_str_two_chinese, last_one,session)
    //    end

    // 不过滤关系了,以前返回的是end
    last_one
  }

  /**
    * 完全匹配
    **/
  def all_pipei(robot_data_json: Array[String], str: String): JSONArray = {
    val response_Json_array: JSONArray = new JSONArray()

    val result_order = robot_data_json.map(x => {
      val json_res = JSON.parseObject(x)
      val query = json_res.getString("query")
      (query, x)
    }).filter(_._1.contains(str)).take(1).toSeq

    for (i <- result_order.indices) {
      val result = JSON.parseObject(result_order(i)._2)

      val response_Json = new JSONObject()
      response_Json.put("query", result.getString("query"))
      response_Json.put("answer", result.getString("answer"))
      response_Json.put("id", result.getString("id").toInt)
      response_Json.put("summary", result.getString("summary"))
      response_Json.put("type", result.getString("type"))
      response_Json.put("keywords", result.getString("keywords"))
      response_Json.put("level", 1) //等级:1是最优先的
      response_Json_array.add(response_Json)
    }
    response_Json_array
  }


  //工种得到预测的结果完全能对的上的
  def getProduct_result_work_all(robot_data_json_before: Array[String], str: String): Array[((String, String), Int)]
  = {
    val robot_data_json = robot_data_json_before.filter(x => {
      val query = JSON.parseObject(x).getString("worktype_name")
      val p = Pattern.compile("[\u4e00-\u9fa5]")
      val m = p.matcher(query)
      if (m.find()) true else false
    })
    val result_value: Array[((String, String), Int)] = robot_data_json.map(x => {
      val json_res = JSON.parseObject(x)
      val query = json_res.getString("worktype_name")
      ((query, x), query.length)
    }).filter(_._1._1.contains(str))
      result_value

  }

  //工种得到预测的结果案情
  def getProduct_result_work(robot_data_json_before: Array[String], result_str: String, stop: StopRecognition): Array[(String, Int, String, String, String, String, String)]
  = {
    //鉴定是否为空
    def if_not_null(str: String): String = {
      if (str == null || str == "null" || str == "") "" else str
    }

    val robot_data_json = robot_data_json_before.filter(x => {
      val query = JSON.parseObject(x).getString("worktype_name")
      val p = Pattern.compile("[\u4e00-\u9fa5]")
      val m = p.matcher(query)
      if (m.find()) true else false
    })

    val sentenceData = robot_data_json.map(x => JSON.parseObject(x).getString("worktype_name")) //找到我历史数据中得值
    val res: Set[String] = sentenceData.flatMap(x => DicAnalysis.parse(x).recognition(stop).toStringWithOutNature(" ").split(" ")).toSet

    //得到我这个案件描述中，都包含哪些个关键字
    val result: Array[String] = result_str.split(" ")
      .map(x => {
        val char_size = x.getBytes("UTF-8").length
        (x, char_size)
      }) //.filter(_._2 > 3)
      .map(_._1).filter(res.contains)
    //找出我历史数据中出现的次数
    val result_value = robot_data_json.map(x => {
      val json_res = JSON.parseObject(x)
      val query = json_res.getString("worktype_name")
      val result_str = DicAnalysis.parse(query.replaceAll("\\d+", "")).recognition(stop).toStringWithOutNature(" ").split(" ")
      (query, result_str, x)
    }).filter(line => line._2.exists(result.contains(_))).map(x => {
      val json_res = JSON.parseObject(x._3)
      val worktype_code = json_res.getString("worktype_code")
      val profession = json_res.getString("profession")
      val ai_level = json_res.getString("ai_level")
      val insurance_company = json_res.getString("insurance_company")

      val s = x._2.toSet & result.toSet //求得交集

      //x._2.toSet(知识库中每个问题，拆分完后出现了几个分词)
      //result.toSet(问的问题，拆分完后出现了几个分词)
      val person_query = result.toSet.size.toDouble //问的问题拆分完后的词的个数
      /**
        * 先计算出我问的问题，拆分完后与知识库中拆分完后的占比 再 乘以 （它们共有的分词个数/问的问题拆分完后的个数)
        **/
      val value_ratio = (person_query / x._2.toSet.size) * (s.size / person_query)
      (x._1, s.size, value_ratio.formatted("%.2f"), worktype_code, profession, ai_level, insurance_company)
    })
    val value_max = if (result_value.length == 0) 0 else result_value.map(_._2).max
    //过滤出出现次数最多的值
    val end: Array[(String, Int, String, String, String, String, String)] = result_value.filter(x => if (x._2 == value_max) true else false)
    end
  }

  //工种对应表
  def getProduct_result_work_mapping(robot_data_json_before: Array[String], result_str: String, stop: StopRecognition): Array[(String, Int, String, String)]
  = {
    //鉴定是否为空
    def if_not_null(str: String): String = {
      if (str == null || str == "null" || str == "") "" else str
    }

    val robot_data_json = robot_data_json_before.filter(x => {
      val query = JSON.parseObject(x).getString("work_type")
      val p = Pattern.compile("[\u4e00-\u9fa5]")
      val m = p.matcher(query)
      if (m.find()) true else false
    })


    val sentenceData = robot_data_json.map(x => JSON.parseObject(x).getString("work_type")) //找到我历史数据中得值
    val res: Set[String] = sentenceData.flatMap(x => DicAnalysis.parse(x).recognition(stop).toStringWithOutNature(" ").split(" ")).toSet

    //得到我这个案件描述中，都包含哪些个关键字
    val result: Array[String] = result_str.split(" ")
      .map(x => {
        val char_size = x.getBytes("UTF-8").length
        (x, char_size)
      }) //.filter(_._2 > 3)
      .map(_._1)
      .filter(res.contains)


    //找出我历史数据中出现的次数
    val result_value = robot_data_json.map(x => {
      val json_res = JSON.parseObject(x)
      val query = json_res.getString("work_type")
      val answer = json_res.getString("work_mapping")
      val result_str = DicAnalysis.parse(query.replaceAll("\\d+", "")).recognition(stop).toStringWithOutNature(" ").split(" ")
      (query, result_str, answer, x)
    }).filter(line => line._2.exists(result.contains(_))).map(x => {
      val json_res = JSON.parseObject(x._4)
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
    val value_max = if (result_value.length == 0) 0 else result_value.map(_._2).max
    //过滤出出现次数最多的值
    val end: Array[(String, Int, String, String)] = result_value.filter(x => if (x._2 == value_max) true else false)
    end
  }

  //工种使用知识库并获得答案返回
  def getAnswer_work(stop: StopRecognition, just_one: String, end_case_work_mapping: Array[(String, Int, String, String)]): JSONArray
  = {
    val response_Json_array: JSONArray = new JSONArray()
    //排序:只取得前6个
    val result_order = end_case_work_mapping.sortBy(x => x._4.toDouble).reverse.take(5) //foreach(println(_))
    for (i <- result_order.indices) {
      val response_Json = new JSONObject()
      response_Json.put("workType", result_order(i)._1)
      response_Json.put("workMapping", result_order(i)._3)
      response_Json_array.add(response_Json)
    }
    response_Json_array

  }
}
