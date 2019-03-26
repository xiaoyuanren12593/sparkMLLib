package com.bzn.cLable

import java.util.Properties

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object CLabel {

  def main(args: Array[String]): Unit = {
    //得到标签数据
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf_spark = new SparkConf().setAppName(getClass.getName)
    conf_spark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf_spark.set("spark.sql.broadcastTimeout", "36000")
//      .setMaster("local[4]")

    val sc: SparkContext = new SparkContext(conf_spark)
    val sqlContext: HiveContext = new HiveContext(sc)
    //读取渠道表
    val lines_source = Source.fromURL(getClass.getResource("/config_scala.properties")).getLines.toSeq
    val location_mysql_url: String = lines_source(4).toString.split("==")(1)
    val location_mysql_url_dwdb: String = lines_source(6).toString.split("==")(1)
    val location_mysql_url_test: String = lines_source(7).toString.split("==")(1)
    val prop: Properties = new Properties

    //雇主、接口、ofo数据
    val employerAndInterAndOfo = getEmployerAndInterAndOfo(sqlContext: HiveContext,location_mysql_url: String,location_mysql_url_test:String,prop: Properties)
    //    employerAndInterAndOfo.show(10)

    //从hive 中获得ofo数据
    val ofoData = getOfoFromHive(sqlContext: HiveContext)

    //从mysql中获取雇主体育健康员福的数据
    val eSHMData = getEmployerAndSportAndHealthAndMemberFromMysql(location_mysql_url_dwdb: String,location_mysql_url,sqlContext: HiveContext,prop)

    //将产品拉平
    val eSHMDataFlat = flatProductESHMData(sqlContext,eSHMData :DataFrame)
    //    eSHMDataFlat.show(20)

    //获取接口数据
    val interfaceData = getInterfaceData(sqlContext: HiveContext,location_mysql_url_test: String,location_mysql_url_dwdb: String,prop: Properties )
    //    interfaceData.show(10)

    //接口数据拉平
    val productInterfaceDataFlat = flatProductInterfaceData(sqlContext,interfaceData :DataFrame)
    //    productInterfaceDataFlat.show(10)

    //雇主数据和总表join
    val resOne = employerAndInterAndOfo.join(eSHMDataFlat,employerAndInterAndOfo("insured_mobile") === eSHMDataFlat("insured_mobile_emp"),"leftouter")
    //    println(resOne.count())
    //接口数据和总表join
    val resTwo = resOne.join(productInterfaceDataFlat,resOne("insured_mobile") === productInterfaceDataFlat("insured_mobile_int"),"leftouter")

    //ofo数据和总表join
    val res = resTwo.join(ofoData,resTwo("insured_mobile") === ofoData("insured_mobile_ofo"),"leftouter")
      .select("insured_name","insured_cert_no","insured_mobile","hr","sport","health","sence","education","employer","omnipotent","share","otherSence","partTimeSence","wedding","xDuZhongbao","xYang","findRun","freight","mango","ofo")
      .distinct()
    //    res.show(10)
    //写入hive
    res.insertInto("odsdb_prd.employer_interface_ofo_c",overwrite = true)

//    val path = s"/share/ods/employer_interface_ofo_c"
//
//    res.map(x => x.mkString("\\u0001")).repartition(1).saveAsTextFile(path)
    //    val table = "odsdb_prd.employer_interface_ofo_c"
    //    val output_tmp_dir = ""
    //
    //    sqlContext.sql(s"""load data inpath '$output_tmp_dir' overwrite into table $table """)
    //关闭上下文
    sc.stop()
  }


  /**
    * 接口数据拉平
    * @param sqlContext
    * @param interfaceData
    */
  def flatProductInterfaceData(sqlContext: HiveContext, interfaceData: DataFrame) = {
    import sqlContext.implicits._
    val res = interfaceData.map(x =>{
      var omnipotent = 0 //万能小哥
      var share = 0 //共享单车
      var otherSence = 0 //其他场景
      var partTimeSence = 0 //兼职场景
      var wedding = 0 //婚礼纪
      var xDuZhongbao = 0 //小度众包
      var xYang = 0 //新氧医美
      var findRun = 0 //觅跑
      var freight = 0 //货运
      var mango = 0 //青芒果
      val insured_mobile = x.getAs[String]("insured_mobile")
      val product_new_2 = x.getAs[String]("product_new_2")
      val split = product_new_2.split("\\u0001").array.distinct
      for (elem <- split) {
        if(elem == "万能小哥"){
          omnipotent = 1
        }else if(elem == "共享单车"){
          share = 1
        }else if(elem == "其他场景"){
          otherSence = 1
        }else if(elem == "兼职场景"){
          partTimeSence = 1
        }else if(elem == "婚礼纪"){
          wedding = 1
        }else if(elem == "小度众包"){
          xDuZhongbao = 1
        }else if(elem == "新氧医美"){
          xYang = 1
        }else if(elem == "觅跑"){
          findRun = 1
        }else if(elem == "货运"){
          freight = 1
        }else if(elem == "青芒果"){
          mango = 1
        }
      }

      (insured_mobile,omnipotent,share,otherSence,partTimeSence,wedding,xDuZhongbao,xYang,findRun,freight,mango)
    })
      .toDF("insured_mobile_int","omnipotent","share","otherSence","partTimeSence","wedding","xDuZhongbao","xYang","findRun","freight","mango")
      .filter("length(insured_mobile_int)>0")
      .distinct()
    res
  }

  /**
    * 获取接口数据
    * @param sqlContext
    * @param location_mysql_url_test
    * @param location_mysql_url_dwdb
    * @param prop
    */
  def getInterfaceData(sqlContext: HiveContext, location_mysql_url_test: String, location_mysql_url_dwdb: String,prop: Properties) = {
    //读取open_other_policy_temp
    val open_other_policy_temp = sqlContext.read.jdbc(location_mysql_url_test, "open_other_policy_temp", prop)
      .select("insured_name","insured_cert_no","insured_mobile","product_code")
      .filter("product_code is not null")

    //读取产品表
    val dim_product = sqlContext.read.jdbc(location_mysql_url_dwdb, "dim_product", prop)
      .select("product_code","product_new_2")
      .filter("product_code is not null")

    //open_other_policy_temp和dim_product做join
    val res = open_other_policy_temp.join(dim_product,open_other_policy_temp("product_code") === dim_product("product_code"))
      .select("insured_mobile","product_new_2")
      .distinct()

    res
  }

  //将产品拉平
  def flatProductESHMData(sqlContext:SQLContext,eSHMData: DataFrame) = {
    import sqlContext.implicits._
    val res = eSHMData.map(x =>{
      var hr = 0 //人力资源
      var sport = 0 //体育
      var health = 0 //健康
      var sence = 0 //场景
      var education = 0 //教育
      var employer = 0 //雇主
      val insured_mobile = x.getAs[String]("insured_mobile")
      val product_new_1 = x.getAs[String]("product_new_1")
      val split = product_new_1.split("\\u0001").array.distinct
      for (elem <- split) {
        if(elem == "人力资源"){
          hr = 1
        }else if(elem == "体育"){
          sport = 1
        }else if(elem == "健康"){
          health = 1
        }else if(elem == "场景"){
          sence = 1
        }else if(elem == "教育"){
          education = 1
        }else if(elem == "雇主"){
          employer = 1
        }
      }
      (insured_mobile,hr,sport,health,sence,education,employer)
    })
      .toDF("insured_mobile_emp","hr","sport","health","sence","education","employer")
      .filter("length(insured_mobile_emp) > 0")
      .distinct()
    res
  }

  /**
    * 从mysql中获取雇主体育健康员福的数据
    * @param location_mysql_url
    * @param sqlContext
    */
  def getEmployerAndSportAndHealthAndMemberFromMysql(location_mysql_url_dwdb: String,location_mysql_url: String,sqlContext: HiveContext,prop: Properties) = {

    import sqlContext.implicits._
    //读取在保人表
    val ods_policy_insured_detail = sqlContext.read.jdbc(location_mysql_url, "ods_policy_insured_detail", prop)
      .select("insured_name","insured_cert_no","insured_mobile","policy_id")
      .filter("policy_id is not null")

    //读取保单表
    val ods_policy_detail = sqlContext.read.jdbc(location_mysql_url, "ods_policy_detail", prop)
      .select("insure_code","policy_id")
      .filter("policy_id is not null")

    //保单表和保全表做join
    val tempOne = ods_policy_insured_detail.join(ods_policy_detail,ods_policy_insured_detail("policy_id") === ods_policy_detail("policy_id"))
      .select("insured_name","insured_cert_no","insured_mobile","insure_code")
      .filter("insure_code is not null")

    //读取产品表
    val dim_product = sqlContext.read.jdbc(location_mysql_url_dwdb, "dim_product", prop)
      .select("product_code","product_new_1")
      .filter("product_code is not null")

    //tempOne结果集和产品表通过产品编号做join
    val resOne = tempOne.join(dim_product,tempOne("insure_code") === dim_product("product_code"))
      .select("insured_name","insured_cert_no","insured_mobile","product_new_1")
      .filter("insured_mobile is not null")
      .distinct()
    val resTwo = resOne.map(x => {
      val insured_mobile = x.getAs[String]("insured_mobile")
      val product_new_1 = x.getAs[String]("product_new_1")
      (insured_mobile,product_new_1)
    })
      .reduceByKey((x1,x2) => {
        x1+"\\u0001"+x2
      })
      .map(x => {
        (x._1,x._2)
      })
      .toDF("insured_mobile","product_new_1")
      .distinct()

    resTwo
  }

  /**
    * 从hive 中获得ofo数据
    * @param sqlContext
    */
  def getOfoFromHive(sqlContext: HiveContext): DataFrame = {

    import sqlContext.implicits._

    //读取ofo表的数据
    val ofoData: DataFrame = sqlContext.sql("select name,cert_no,mobile,num from bzn_open.odr_policy_insured_distinct")
      .map(x => {
        val insured_mobile = x.getAs[String]("mobile")
        val ofo = x.getAs[Int]("num").toString
        (insured_mobile,ofo)
      })
      .toDF("insured_mobile_ofo","ofo")
      .filter("length(insured_mobile_ofo) >0")
      .distinct()
    ofoData
  }

  /**
    * 雇主、接口、ofo数据
    * @param sqlContext
    * @param location_mysql_url
    * @param location_mysql_url_test
    * @param prop
    * @return
    */
  def getEmployerAndInterAndOfo(sqlContext: HiveContext, location_mysql_url: String, location_mysql_url_test: String, prop: Properties) = {
    //读取在保人表
    val ods_policy_insured_detail = sqlContext.read.jdbc(location_mysql_url, "ods_policy_insured_detail", prop)
      .select("insured_name","insured_cert_no","insured_mobile")

    val open_other_policy_temp = sqlContext.read.jdbc(location_mysql_url_test, "open_other_policy_temp", prop)
      .select("insured_name","insured_cert_no","insured_mobile")

    val odr_policy_insured_distinct = sqlContext.sql("select name,cert_no,mobile from bzn_open.odr_policy_insured_distinct")
      .toDF("insured_name","insured_cert_no","insured_mobile")

    val res = ods_policy_insured_detail.unionAll(open_other_policy_temp).unionAll(odr_policy_insured_distinct)
      .filter("length(insured_mobile) >0")
      .distinct()
    res
  }


}