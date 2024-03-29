package com.bzn.sparkUtil

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

trait SparkUtil {
  /**
    * spark配置信息以及上下文
    * @param appName 名称
    * @param exceType 执行类型，本地/集群
    */
  def sparkConfInfo(appName:String,exceType:String): (SparkConf, SparkContext, SQLContext, HiveContext) ={
    val conf = new SparkConf()
      .setAppName(appName)
     if(exceType != ""){
       conf.setMaster(exceType)
     }else {
       conf.setMaster("local[*]")
     }

    val sc = new SparkContext(conf)

    val sqlContest = new SQLContext(sc)

    val hiveContext = new HiveContext(sc)

    (conf,sc,sqlContest,hiveContext)
  }

}
