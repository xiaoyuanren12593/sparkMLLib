package com.bzn.cLabel

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 联系人详细信息和手机归属地进行关联得到联系人的详细信息
  * create user xingyuan
  * date 2019-4-5
  */
object MobileEmpInterOfoTest {

  def main(args: Array[String]): Unit = {
    //得到标签数据
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf_spark = new SparkConf().setAppName(getClass.getName)
    conf_spark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf_spark.set("spark.sql.broadcastTimeout", "36000")
      .setMaster("local[4]")

    val sc: SparkContext = new SparkContext(conf_spark)
    val sqlContext: HiveContext = new HiveContext(sc)

    val empInterOfoData = getEmpInterOfoData(sqlContext)

    val resmMobile = getProducts(empInterOfoData:DataFrame,sqlContext:HiveContext)
//    resmMobile.show(1000)

//    val outputTmpDir = "/share/cTable_emp_inter_ofo_mobile"
//    val output = "odsdb_prd.emp_inter_ofo_mobile"
//
//    resmMobile.rdd.map(x => x.mkString("\001")).repartition(1).saveAsTextFile(outputTmpDir)
//    sqlContext.sql(s"""load data  inpath '$outputTmpDir' overwrite into table $output""")

    sc.stop()

  }

  /**
    * 得到手机号条件下的数据
    * @param empInterOfoData
    * @param sqlContext
    */
  def getProducts(empInterOfoData: DataFrame, sqlContext: HiveContext) = {
    import sqlContext.implicits._
    val mobileEmpInterOfoData = empInterOfoData.select("insured_cert_no","insured_mobile","product_type")
      .where("insured_mobile is not null")
      .map(x => {
        var insured_cert_no = x.getAs[String]("insured_cert_no")
        if(insured_cert_no == null){
          insured_cert_no = ""
        }
        val insured_mobile = x.getAs[String]("insured_mobile")
        val product_type = x.getAs[String]("product_type")
        (insured_mobile,(insured_cert_no,product_type))
      })
      .reduceByKey((x1,x2) => {
        x1._1+"\u0001"+x2._1
        x1._2+"\u0001"+x2._2
        (x1._1,x1._2)
      })
      .map(x => {
        val certLength = x._2._1.trim.split("\u0001").distinct.length
        val product_types = x._2._2.split("\u0001").distinct.mkString("\u0001")
        if(certLength <=1){
          val certs = x._2._1.split("\u0001").distinct
          if(certLength == 0){
            (null,x._1,product_types)
          }else{
            if(certs(0) == ""){
              (null,x._1,product_types)
            }else{
              (certs(0),x._1,product_types)
            }
          }
        }else{
          ("","","")
        }
      })
      .filter(x => x._2 != "")
      .toDF("insured_cert_no","insured_mobile_b","product_types")
    val products = empInterOfoData.join(mobileEmpInterOfoData,empInterOfoData("insured_mobile") === mobileEmpInterOfoData("insured_mobile_b"),"leftouter")
      .map(x => {
        val insured_name = x.getAs[String]("insured_name")
        val insured_cert_no = x.getAs[String]("insured_cert_no")
        val insured_mobile = x.getAs[String]("insured_mobile_b")
        val product_types = x.getAs[String]("product_types")
        if(product_types == null){
          (insured_name,insured_cert_no,insured_mobile,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)
        }else {
          val products = productTypeSplits(product_types)
          (insured_name,insured_cert_no,insured_mobile,products._1,products._2,products._3,products._4,products._5
            ,products._6,products._7,products._8,products._9,products._10,products._11,products._12,products._13,products._14,products._15,products._16,products._17,products._18,products._19)
        }
      })
      .toDF("insured_name","insured_cert_no","insured_mobile","hr","sport","health","sence","education","employer","omnipotent","share","otherSence","partTimeSence","wedding","xDuZhongbao","xYang","findRun","freight","mango","client58","courier58","ofo")
      .distinct()

    products
  }
  /**
    * 获取雇主、接口、ofo等数据
    * @param sqlContext
    * @return
    */
  def getEmpInterOfoData(sqlContext:HiveContext) = {
    import sqlContext.implicits._
    val empInterOfo = sqlContext.sql("select insured_name,insured_cert_no,insured_mobile,product_type from odsdb_prd.emp_inter_ofo")
      .map(x => {
        val insured_name = x.getAs[String]("insured_name")
        var insured_cert_no = x.getAs[String]("insured_cert_no")
        var insured_mobile = x.getAs[String]("insured_mobile")
        val product_type = x.getAs[String]("product_type")
        if(insured_cert_no != null){
          insured_cert_no = insured_cert_no.trim
        }
        if(insured_mobile != null){
          insured_mobile = insured_mobile.trim.replace(" ","")
        }
        (insured_name,insured_cert_no,insured_mobile,product_type)
      })
      .toDF("insured_name","insured_cert_no","insured_mobile","product_type")
      .distinct()
    empInterOfo
  }

  /**
    * 将产品分割
    * @param product_types
    * @return
    */
  def productTypeSplits(product_types:String) ={
    //对产品进行切分
    val splits = product_types.split("\u0001")

    val products = Array("人力资源","体育","健康","场景","教育","雇主","万能小哥","共享单车","其他场景","兼职场景","婚礼纪","小度众包"
      ,"新氧医美","觅跑","货运","青芒果","58速运-客户","58速运-司机")
    var hr = 0 //人力资源
    var sport = 0 //体育
    var health = 0 //健康
    var sence = 0 //场景
    var education = 0 //教育
    var employer = 0 //雇主
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
    var client58 = 0 //58速运-客户
    var courier58 = 0//58速运-司机
    var ofo = 0//ofo
    for(item <- splits){
      if(item == "人力资源"){
        hr = 1
      }else if(item == "体育"){
        sport = 1
      }else if(item == "健康"){
        health = 1
      }else if(item == "场景"){
        sence = 1
      }else if(item == "教育"){
        education = 1
      }else if(item == "雇主"){
        employer = 1
      }else if(item == "万能小哥"){
        omnipotent = 1
      }else if(item == "共享单车"){
        share = 1
      }else if(item == "其他场景"){
        otherSence = 1
      }else if(item == "兼职场景"){
        partTimeSence = 1
      }else if(item == "婚礼纪"){
        wedding = 1
      }else if(item == "小度众包"){
        xDuZhongbao = 1
      }else if(item == "新氧医美"){
        xYang = 1
      }else if(item == "觅跑"){
        findRun = 1
      }else if(item == "货运"){
        freight = 1
      }else if(item == "青芒果"){
        mango = 1
      }else if(item == "58速运-客户"){
        client58 = 1
      }else if(item == "58速运-司机"){
        courier58 = 1
      }else if(item == "ofo"){
        ofo = 1
      }
    }
    (hr,sport,health,sence,education,employer,omnipotent,share,otherSence,partTimeSence,wedding,xDuZhongbao,xYang,findRun,freight,mango,client58,courier58,ofo)
  }
}
