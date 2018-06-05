package company.sparkMLlib

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

//spark读取MySQL的数据存到HDFS中

object Spark_connect_mysql {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wuYsu").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val url = "jdbc:mysql://172.16.11.105:3306/odsdb?user=odsuser&password=odsuser"

    val prop = new Properties()
    val df_ods_policy_insured_detail: DataFrame = sqlContext.read.jdbc(url, "ods_policy_insured_detail", prop)
    val df_ods_policy_detail: DataFrame = sqlContext.read.jdbc(url, "ods_policy_detail", prop)

    val df_d_worktype_za: DataFrame = sqlContext.read.jdbc(url, "d_worktype_za", prop)
    val df_d_work_maping: DataFrame = sqlContext.read.jdbc(url, "d_work_maping", prop)

    val temp_worktype_20180316: DataFrame = sqlContext.read.jdbc(url, "temp_worktype_20180316", prop)
    temp_worktype_20180316.map(x => {
      x(0)
    })
      .repartition(1).saveAsTextFile("hdfs://namenode1.cdh:8020/spark/temp_worktype_20180316")
    //d_worktype_za
    /**
      * StructType(StructField(id,StringType,true),
      * StructField(stype,StringType,true),
      * StructField(mtype,StringType,true),
      * StructField(ltype,StringType,true),
      * StructField(li_level,IntegerType,true),
      * StructField(ai_level,IntegerType,true),
      * StructField(mi_level,IntegerType,true))
      **/
    //ods_policy_insured_detail
    /**
      * StructType(StructField(id,StringType,false),
      * StructField(insured_id,StringType,true),
      * StructField(policy_id,StringType,true),
      * StructField(insured_type,StringType,true),
      * StructField(is_legal,BooleanType,true),
      * StructField(insured_name,StringType,true),
      * StructField(insured_gender,BooleanType,true),
      * StructField(insured_cert_type,BooleanType,true),
      * StructField(insured_cert_no,StringType,true),
      * StructField(insured_birthday,DateType,true),
      * StructField(insured_profession,StringType,true),
      * StructField(insured_mobile,StringType,true),
      * StructField(insured_industry,StringType,true),
      * StructField(insured_work_type,StringType,true),
      * StructField(insured_nation,StringType,true),
      * StructField(insured_company_name,StringType,true),
      * StructField(insured_company_phone,StringType,true),
      * StructField(is_chief_insurer,BooleanType,true),
      * StructField(insured_status,IntegerType,true),
      * StructField(insure_policy_status,IntegerType,true),
      * StructField(insured_create_time,TimestampType,true),
      * StructField(insured_update_time,TimestampType,true),
      * StructField(insured_start_date,TimestampType,true),
      * StructField(insured_end_date,TimestampType,true),
      * StructField(child_id,StringType,true),
      * StructField(child_name,StringType,true),
      * StructField(child_gender,BooleanType,true),
      * StructField(child_cert_type,BooleanType,true),
      * StructField(child_cert_no,StringType,true),
      * StructField(child_birthday,TimestampType,true),
      * StructField(child_nationality,StringType,true),
      * StructField(child_policy_status,IntegerType,true),
      * StructField(child_start_date,TimestampType,true),
      * StructField(child_end_date,TimestampType,true), S
      * tructField(child_create_time,TimestampType,true),
      * StructField(age,IntegerType,true))
      *
      **/
    //d_work_maping
    /**
      * StructType(StructField(work_type,StringType,true),
      * StructField(id,StringType,true),
      * StructField(work_mapping,StringType,true),
      * StructField(mtype,StringType,true),
      * StructField(ltype,StringType,true),
      * StructField(ai_level,IntegerType,true))
      **/
    val d_worktype_za = df_d_worktype_za.map(x => {
      s"${x(0)},${x(1)},${x(2)},${x(3)},${x(4)},${x(5)},${x(6)}"
    })

    val ods_policy_insured_detail = df_ods_policy_insured_detail.map(x => {
      s"${x(2)},${x(13)}"
    }).filter(x => if (x != null) true else false)
    //          .repartition(1)
    //          .saveAsTextFile("hdfs://namenode1.cdh:8020/spark/ods_policy_insured_detail")

    val d_work_maping = df_d_work_maping.map(x => {
      s"${x(0)},${x(1)},${x(2)},${x(3)},${x(4)}"
    })

    val ods_policy_detail = df_ods_policy_detail.map(x => x)
    //      .repartition(1).saveAsTextFile("hdfs://namenode1.cdh:8020/spark/ods_policy_detail")


  }
}
