package robot_data

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.DicAnalysis
import org.neo4j.driver.v1._

import scala.util.control.Breaks._


//case class User(name: String, last_name: String, age: Int, city: String)

object TheMatrixTraversalHack {


  //  val driver: Driver = GraphDatabase.driver("bolt://datanode2.cdh:7687", AuthTokens.basic("neo4j", "123456"))
  val driver: Driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "maokai"))
  val session: Session = driver.session

  //Create a Node :
  def createNode(str: String): Unit = {
    //    val script = s"CREATE (user:Users {name:'${user.name}',last_name:'${user.last_name}',age:${user.age},city:'${user.city}'})"
    //    val script = s"CREATE (user:Users {name:'mk',last_name:'last_mk',age:24,city:'beijing'})"
    //    val script = s"CREATE (a:class{name:'体育官网'})-[r:ACTED_IN{roles:'运动'}]->(n:sports {name:'$str'}) return n"

    /**
      * unwind可行:
      * unwind [{year:2014,id:1},{year:2014,id:2}] as events
      * create (n:person{name:events.year})
      * return n
      * 加载后去重: load csv from 'file:///Neo4j.csv' as line with distinct line[0] as yundong  create(n:sports{name: yundong}) return n
      **/

    val ss = "load csv from 'file:///Neo4j.csv' as line  create()"


    val script = s"unwind [$str] as events" +
      " create (n:sports{name:events})" +
      " return n"
    session.run(script)
  }

  //Retrieve(检索)该节点的所有数据 :
  def Retrieve(): Unit = {
    try {
      //    val script = "MATCH (user:Users) RETURN user.name AS name, user.last_name AS last_name, user.age AS age, user.city AS city"
      val script = "MATCH (n:Dept) RETURN n.location as location ,n.dname as dname ,n.deptno as deptno "
      val result: StatementResult = session.run(script)

      var map_s: Map[Int, Array[(String, String)]] = Map(0 -> Array(("", ""))) //初始化构造函数
      var s = Array[(String, String)]()
      var i = 0
      //读取该节点中的所有属性,并将该节点中的所有属性存放到map_s中
      while (result.hasNext) {
        val record = result.next()
        s = record.keys.toArray.map(_.toString).zip(record.values.toArray.map(_.toString))
        map_s += (i -> s)
        i = i + 1
      }
      map_s.foreach(x => println((x._1, x._2.mkString(";"))))
      session.close()
      driver.close()
    } catch {
      case e: Exception => e.getMessage
    }
  }


  //Update a Node :
  def Update(): Unit = {
    //            val script =s"MATCH (user:Users) where user.name ='$name' SET user.name = '$newName' RETURN user.name AS name, user.last_name AS last_name, user.age AS age, user.city AS city"
    //      val script = s"MATCH (user:Users) where user.name ='mk' SET user.name = 'new_mk' RETURN user.name AS name, user.last_name AS last_name, user.age AS age, user.city AS city"
    //    session.run(script)
  }

  //  Delete a Node :
  def Delete(): Unit = {
    val script = s"MATCH (user:Users) where user.name ='new_mk' Delete user"
    val result = session.run(script)
  }


  //查询neo4j自动生成的id值
  def select(): Unit = {
    try {
      //    val script = "MATCH (user:Users) RETURN user.name AS name, user.last_name AS last_name, user.age AS age, user.city AS city"
      val name_value = "vic"
      val script = s"match(n:Person{name:'$name_value'}) return id(n) as id,n.born,n.name" //通过id函数找到该节点得ID
      val result: StatementResult = session.run(script)

      var map_s: Map[String, Array[(String, String)]] = Map("" -> Array(("", ""))) //初始化构造函数
      var s = Array[(String, String)]()
      //读取该节点中的所有属性,并将该节点中的所有属性存放到map_s中
      while (result.hasNext) {
        val record = result.next()
        val id = record.get("id").toString //该id为neo4j自动生成的id值
        s = record.keys.toArray.map(_.toString).zip(record.values.toArray.map(_.toString))
        map_s += (id -> s)
      }
      map_s.foreach(x => println((x._1, x._2.mkString(";"))))
      session.close()
      driver.close()
    } catch {
      case e: Exception => e.getMessage
    }
  }

  //创建关系
  def createRelation(): Unit = {
    val script = s"MATCH (n:ceshi{name:'Michael Douglas'}),(a:ceshi{name:'Michaeljuypi'}) create (n)-[rs:DIRECTED]->(a) return rs"
    val result = session.run(script)
  }


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
    val end: Array[(String, String)] = json_arr.toArray().map(x => JSON.parseObject(x.toString)).map(x => (x.getString("node_name"), x.getString("id"))).filter(x => tep_one.contains(x._1))
    end
  }

  //分词准备
  val stop: StopRecognition = new StopRecognition
  stop.insertStopNatures("w") //过滤掉标点
  //    val category = "业务以及流程知识"

  def main(args: Array[String]): Unit = {


    val str = "击剑？" //体育,官,网,在,哪里
    val result_str: String = DicAnalysis.parse(str).recognition(stop).toStringWithOutNature(" ")

    val json_arr_relation: JSONArray = new JSONArray()

    var format_data = "1445,1446,1450,1452"
    breakable {
      for (a <- 1 to 4) {
        if (a == 3 || format_data == "10") break; //当a等于3时跳出breakable块
        val relation_node = session.run(s"unwind[$format_data] as line match(n)-[r] ->(m) where id(n)=line  return  n.name as local_name , type(r) as relation_ships_node_type ,m.name as relation_ships_node_name")
        val relation_ships_and_node = new JSONObject()

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
        val tep_one: Array[String] = json_arr_relation.toArray.map(x => JSON.parseObject(x.toString)).map(x => x.getString("relation_ships_node_name"))
        format_data = get_id(tep_one).map(_._2).mkString(",")
        println("循环第" + a + "次")
        val sa = json_arr_relation.toArray().distinct.map(x => {
          (JSON.parseObject(x.toString).getString("relation_ships_node_name"), x.toString)
        })
        //有重复退出，循环
        if (sa.map(_._1).toSet.size != sa.length) format_data = "10"
        //将重复的过滤出来
        sa.groupBy(_._1).filter(x => x._2.length > 1).map(x => {
          x._2.mkString(";")
        }).foreach(println(_))
      }
    }
    //    json_arr_relation.toArray().distinct.foreach(println(_))


    /**
      * foreach在Neo4j中的用法
      * FOREACH (name in ["Johan","Rajesh","Anna","Julia","Andrew"] |
      * CREATE (:product {name:name}))
      **/


    /**
      * 运动-产品名称-投保方式创建节点
      * load csv from 'file:///Neo4j.csv' as line
      * with distinct line[1] as yundong
      * create (b:sports{name:yundong})
      * return b
      **/

    /**
      * 产品分类创建节点
      * load csv from 'file:///Neo4j.csv' as line
      * with distinct line[4] as yundong
      * create (b:produce_class{name:line[4],produce_anme:,name:,mode_name:,sports_name})
      * return b
      *
      **/

    /**
      * 运动-产品名称-投保方式之间创建关系
      * match(a:mk01),(b:product)
      * where b.value=a.name
      * create (a)-[r:ACTED_IN{roles:"包括"}]->(b)
      * return r
      **/

    /**
      * 产品分类的创建关系
      * match(a:mode),(b:produce_class)
      * where a.name=b.mode_name and a.value=b.produce_name
      * create (a)-[r:产品分类]->(b)
      * return r
      **/


    //创建运动节点加上去重
    /**
      * load csv from 'file:///Neo4j.csv' as line
      * with distinct line[1] as yundong
      * create (b:sports{name:yundong})
      * return b
      *
      **/

    //创建节点的关系(体育与运动) 体育节点以ID进行固定
    /**
      * match(a:class),(b:sports)
      * where id(a)=863
      * create (a)-[r:ACTED_IN{roles:"包括"}]->(b)
      * return r
      *
      **/
    //    Retrieve()
    //    Update()
    //    Delete()
    //    select()
    //    createRelation()

  }
}