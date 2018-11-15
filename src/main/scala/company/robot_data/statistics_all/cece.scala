package company.robot_data.statistics_all

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.log4j.Level
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by MK on 2018/10/15.
  */
object cece {
  def main(args: Array[String]): Unit = {
    val lines_source = Source.fromURL(getClass.getResource("/config_scala.properties")).getLines.toSeq
    org.apache.log4j.Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    //mysql配置
    val location_mysql_robot_url: String = lines_source(3).toString.split("==")(1)
    val location_mysql_url: String = lines_source(2).toString.split("==")(1)


    val conf_s = new SparkConf().setAppName("robot_streaming_test")
      .setMaster("local[4]")
    val sc = new SparkContext(conf_s)
    val sqlContext: HiveContext = new HiveContext(sc)
    val prop: Properties = new Properties


    val before = sqlContext.read.jdbc(location_mysql_url, "ods_policy_worktype_map", prop).map(x => {
      (x.getAs[String]("work_type"), x.getAs[String]("work_mapping"))
    }
    )


    //    sc.textFile("C:\\Users\\a2589\\Desktop\\国寿.txt").map(x => {
    //      val all = JSON.parseObject(x.split("mk6")(1))
    //      val s = all.getString("standardWork")
    //      val insuranceCompany = JSON.parseObject(s).getString("insuranceCompany")
    //      val workName = JSON.parseObject(s).getString("workName")
    //      (x.split("mk6")(0), (workName, insuranceCompany))
    //    }).join(before).map(x=>{
    //     val one =  x._1 //输入信息
    //     val two = x._2._1._1 //标准工种
    //     val three = x._2._1._2//表中公司
    //     val four =  x._2._2//对应表的工种
    //      s"${one}mk6${two}mk6${three}mk6$four"
    //    }).repartition(1).saveAsTextFile("C:\\Users\\a2589\\Desktop\\jieguo\\国寿")


    sc.textFile("C:\\Users\\a2589\\Desktop\\中华.txt").map(x => {
      val all = JSON.parseObject(x.split("mk6")(1))
      val one = x.split("mk6")(0) //输入信息


      if (all != null) {
        val s = all.getString("standardWork")
        val insuranceCompany = JSON.parseObject(s).getString("insuranceCompany")
        val workName = JSON.parseObject(s).getString("workName")
        val two = workName //标准工种

        val three = insuranceCompany //表中公司
        val end = if (one == two) "1" else "2" //1为完全一致2为不完全一致
        s"${one}mk6${two}mk6${three}mk6$end"
      } else {
        s"${one}mk6nullmk6nullmk6null"

      }
    }).repartition(1).saveAsTextFile("C:\\Users\\a2589\\Desktop\\jieguo\\中华")

  }
}
