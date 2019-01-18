package company.year.month

import java.io.File
import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by 邢万成 on 2019/01/16.
  */
object baseinfo_merge_test extends year_until {
  //遍历某目录下所有的文件和子文件
  def subDir(dir: File): Iterator[File]
  = {
    val dirs = dir.listFiles().filter(_.isDirectory())
    val files = dir.listFiles().filter(_.isFile())
    files.toIterator ++ dirs.toIterator.flatMap(subDir)
  }

  def getC3p0DateSource(path: String, table_name: String, url: String): Boolean
  = {
    Class.forName("com.mysql.jdbc.Driver")
    //获取连接//http://baidu.com
    val connection = DriverManager.getConnection(url)
    //通过连接创建statement
    var statement = connection.createStatement()
    val sql1 = s"truncate table odsdb.$table_name"

    val sql2 = s"load data infile '$path'  into table odsdb.$table_name fields terminated by 'mk6'"
    statement = connection.createStatement()
    //先删除数据，在导入数据
    statement.execute(sql1)
    statement.execute(sql2)
  }

  //toMysql
  def toMsql(bzn_year: RDD[String], path_hdfs: String, path: String, table_name: String, url: String): Unit
  = {
    bzn_year.repartition(1).saveAsTextFile(path_hdfs)

    //得到我目录中的该文件
    val res_file = for (d <- subDir(new File(path))) yield {
      if (d.getName.contains("-") && !d.getName.contains(".")) d.getName else "null"
    }
    //得到part-0000
    val end = res_file.filter(_ != "null").mkString("")
    //通过load,将数据加载到MySQL中 : /share/ods_policy_insured_charged_vt/part-0000
    val tep_end = path + "/" + end
    getC3p0DateSource(tep_end, table_name, url)
  }

  //表1.0数据
  def get_one(sqlContext: HiveContext, prop: Properties, url: String): DataFrame
  = {
    //读取odr_policy_insured数据
    sqlContext.read.jdbc(url, "odr_policy_insured", prop)
      .withColumnRenamed("id", "a_id")
      .withColumnRenamed("policy_id", "a_policy_id")
      .withColumnRenamed("insured_type", "a_insured_type")
      .withColumnRenamed("is_legal", "a_is_legal")
      .withColumnRenamed("gender", "insured_gender")
      .withColumnRenamed("cert_type", "insured_cert_type")
      .withColumnRenamed("birthday", "insured_birthday")
      .withColumnRenamed("profession", "insured_profession")
      .withColumnRenamed("mobile", "insured_mobile")
      .withColumnRenamed("industry", "insured_industry")
      .withColumnRenamed("nation", "insured_nation")
      .withColumnRenamed("company_name", "insured_company_name")
      .withColumnRenamed("company_phone", "insured_company_phone")
      .withColumnRenamed("is_chief_insurer", "a_is_chief_insurer")
      .withColumnRenamed("status", "insured_status")
      .withColumnRenamed("insure_policy_status", "a_insure_policy_status")
      .withColumnRenamed("create_time", "insured_create_time")
      .withColumnRenamed("update_time", "insured_update_time")
      .withColumnRenamed("start_date", "insured_start_date")
      .withColumnRenamed("end_date", "insured_end_date")
      .withColumnRenamed("remark","a_remark")
      .registerTempTable("a")

    //读取odr_policy_insured_child数据
    var b = sqlContext.read.jdbc(url, "odr_policy_insured_child", prop)
      .withColumnRenamed("id", "child_id")
      .withColumnRenamed("child_name", "b_child_name")
      .withColumnRenamed("child_gender", "b_child_gender")
      .withColumnRenamed("child_cert_type", "b_child_cert_type")
      .withColumnRenamed("child_cert_no", "b_child_cert_no")
      .withColumnRenamed("child_birthday", "b_child_birthday")
      .withColumnRenamed("child_nationality", "b_child_nationality")
      .withColumnRenamed("child_policy_status", "b_child_policy_status")
      .withColumnRenamed("start_date", "child_start_date")
      .withColumnRenamed("end_date", "child_end_date")
      .withColumnRenamed("create_time", "child_create_time")
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    //读取odr_policy数据
    var c = sqlContext.read.jdbc(url,"odr_policy",prop)
      .withColumnRenamed("id","c_id")
      .withColumnRenamed("user_id","c_user_id")
      .withColumnRenamed("insure_code","c_insure_code")
      .withColumnRenamed("policy_code","c_policy_code")
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    //将sql中的回车替换掉
    sqlContext.sql("select  regexp_replace(name,'\\n','') as a_name , regexp_replace(work_type,'\\n','') as insured_work_type ,regexp_replace(cert_no,'\\n','') as insured_cert_no,* from a")
      .drop("work_type").drop("cert_no").drop("name")
      .registerTempTable("a_new")

    val plc_policy_preserve_insured_version_one: DataFrame = sqlContext.sql("select * from a_new")

    val fields_mk = plc_policy_preserve_insured_version_one.schema.map(x => x.name) :+ "work_type_mk" :+ "name_mk"
    val plc_policy_preserve_insured_version_two = plc_policy_preserve_insured_version_one.map(x => {
      val work = to_null(x.getAs[String]("insured_work_type"))
      val name = to_null(x.getAs[String]("name"))

      (x.toSeq :+ work :+ name).map(x => if (x == null) "null" else x.toString)
    })
    val value = plc_policy_preserve_insured_version_two.map(r => Row(r: _*))
    val schema = StructType(fields_mk.map(fieldName => StructField(fieldName, StringType, nullable = true)))
    val plc_policy_preserve_insured_version_three = sqlContext.createDataFrame(value, schema)
    val plc_policy_preserve_insured =
      plc_policy_preserve_insured_version_three
        .withColumn("insured_work_type", plc_policy_preserve_insured_version_three("work_type_mk"))
        .withColumn("name", plc_policy_preserve_insured_version_three("a_name"))
        .drop("work_type_mk").drop("a_name")

    val tep_one = plc_policy_preserve_insured.join(b,b("insured_id") === plc_policy_preserve_insured("a_id"),"left")

    val tep_two = tep_one.join(c,tep_one("a_policy_id") === c("c_id"),"left")

    tep_two.persist(StorageLevel.MEMORY_AND_DISK_SER)
    var res1 = tep_two.where ("c_user_id not in ('10100080492') or c_user_id is null")
      .where("a_remark not in ('obsolete') or a_remark is null")

    var res2 = tep_two.where("c_insure_code in ('15000001') and c_policy_code like 'BZN%'")

    var res: DataFrame = res1.unionAll(res2)

    var result = res.selectExpr(
      "getUUID() as id",
      "a_id as insured_id",
      "a_policy_id as policy_id",
      "a_insured_type as insured_type",
      "a_is_legal as is_legal",
      "name as insured_name",
      "insured_gender",
      "insured_cert_type",
      "insured_cert_no",
      "insured_birthday",
      "insured_profession",
      "insured_mobile",
      "insured_industry",
      "insured_work_type",
      "insured_nation",
      "insured_company_name",
      "insured_company_phone",
      "a_is_chief_insurer as is_chief_insurer",
      "insured_status",
      "a_insure_policy_status as insure_policy_status",
      "insured_create_time",
      "insured_update_time",
      "insured_start_date",
      "insured_end_date",
      "child_id",
      "b_child_name as child_name",
      "b_child_gender as child_gender",
      "b_child_cert_type as child_cert_type",
      "b_child_cert_no as child_cert_no",
      "b_child_birthday as child_birthday",
      "b_child_nationality as child_nationality",
      "b_child_policy_status as child_policy_status",
      "child_start_date",
      "child_end_date",
      "child_create_time",
      "getAge(insured_cert_no,insured_start_date) as age"
    )
    result
  }

  //表2.0数据
  def get_two(sqlContext: HiveContext, prop: Properties, url: String): DataFrame
  = {

    //读取b_policy_subject_person_master数据
    var b_policy_subject_person_master = sqlContext.read.jdbc(url, "b_policy_subject_person_master", prop)
      .withColumnRenamed("id", "a_id")
      .withColumnRenamed("policy_no", "aa_policy_no")
      .withColumnRenamed("insured_type", "a_insured_type")
      .withColumnRenamed("is_legal", "a_is_legal")
      .withColumnRenamed("sex", "insured_gender")
      .withColumnRenamed("cert_type", "insured_cert_type")
      .withColumnRenamed("cert_no", "a_insured_cert_no")
      .withColumnRenamed("birthday", "insured_birthday")
      .withColumnRenamed("profession_code", "insured_profession")
      .withColumnRenamed("tel", "insured_mobile")
      .withColumnRenamed("industry_code", "insured_industry")
      .withColumnRenamed("nation", "insured_nation")
      .withColumnRenamed("company_name", "insured_company_name")
      .withColumnRenamed("company_phone", "insured_company_phone")
      .withColumnRenamed("create_time", "insured_create_time")
      .withColumnRenamed("update_time", "insured_update_time")
      .withColumnRenamed("start_date", "insured_start_date")
      .withColumnRenamed("end_date", "insured_end_date")

    //读取b_policy_subject_person_slave数据
    var b = sqlContext.read.jdbc(url, "b_policy_subject_person_slave", prop)
      .withColumnRenamed("id", "child_id")
      .withColumnRenamed("policy_no", "b_policy_no")
      .withColumnRenamed("name", "b_child_name")
      .withColumnRenamed("sex", "b_child_gender")
      .withColumnRenamed("cert_type", "b_child_cert_type")
      .withColumnRenamed("cert_no", "b_child_cert_no")
      .withColumnRenamed("birthday", "b_child_birthday")
      .withColumnRenamed("nation", "b_child_nationality")
      .withColumnRenamed("start_date", "child_start_date")
      .withColumnRenamed("end_date", "child_end_date")
      .withColumnRenamed("create_time", "child_create_time")
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    //读取b_policy数据
    var c = sqlContext.read.jdbc(url,"b_policy",prop)
      .withColumnRenamed("id","c_id")
      .withColumnRenamed("policy_no","c_policy_no")
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val b_policy_preservation_subject_person_master_after = b_policy_subject_person_master
    b_policy_preservation_subject_person_master_after.registerTempTable("mk_ce")


    sqlContext.sql("select  regexp_replace(name,'\\n','') as a_name, regexp_replace(work_type,'\\n','') as a_insured_work_type ,regexp_replace(a_insured_cert_no,'\\n','') as a_insured_cert_no_new,* from mk_ce")
      .drop("work_type").drop("a_insured_cert_no").drop("name")
      .registerTempTable("a_new")

    val b_policy_preservation_subject_person_master_version_one: DataFrame = sqlContext.sql("select " +
      "*,CASE WHEN a_new.`status` = '1'" +
      "    THEN '0'" +
      "    ELSE '1'" +
      "  END as insured_status, " +
      "case when (a_new.`status`='1' and insured_end_date > now() and insured_start_date< now() ) then '1' else '2' end as insure_policy_status," +
      "case when insured_cert_type ='1' and insured_start_date is not null then getAge(a_insured_cert_no_new,insured_start_date) else null end as age from a_new") //.persist(StorageLevel.MEMORY_ONLY_SER)

    val fields_mk = b_policy_preservation_subject_person_master_version_one.schema.map(x => x.name) :+ "c_work_type_mk"
    val b_policy_preservation_subject_person_master_version_two = b_policy_preservation_subject_person_master_version_one.map(x => {
      val work = to_null(x.getAs[String]("a_insured_work_type"))
      (x.toSeq :+ work).map(x => if (x == null) "null" else x.toString)
    })

    val value = b_policy_preservation_subject_person_master_version_two.map(r => Row(r: _*))
    val schema = StructType(fields_mk.map(fieldName => StructField(fieldName, StringType, nullable = true)))
    val b_policy_preservation_subject_person_master_version_three = sqlContext.createDataFrame(value, schema)
    val b_policy_preservation_subject_person_master = b_policy_preservation_subject_person_master_version_three.withColumn("insured_work_type", b_policy_preservation_subject_person_master_version_three("c_work_type_mk")).drop("c_work_type_mk")

    b.registerTempTable("b_new")

    val b_2: DataFrame = sqlContext.sql("select *, case when b_new.`status`='1' then '0' else '1' end as child_policy_status from b_new")

    import sqlContext.implicits._
    val tep_one: DataFrame = b_policy_preservation_subject_person_master.join(b_2, b_policy_preservation_subject_person_master("aa_policy_no") === b_2("b_policy_no"), "left")

    val end_final = tep_one.join(c,tep_one("aa_policy_no") === c("c_policy_no"),"left")

    var result = end_final.selectExpr("" +
      "getUUID() as id",
      "a_id as insured_id",
      "c_id as policy_id",
      "'' as insured_type",
      "a_is_legal as is_legal",
      "a_name as insured_name",
      "insured_gender",
      "insured_cert_type",
      "a_insured_cert_no_new as insured_cert_no",
      "insured_birthday",
      "insured_profession",
      "insured_mobile",
      "insured_industry",
      "insured_work_type",
      "insured_nation",
      "insured_company_name",
      "insured_company_phone",
      "'1' as is_chief_insurer",
      "insured_status",
      "insure_policy_status",
      "insured_create_time",
      "insured_update_time",
      "insured_start_date",
      "insured_end_date",
      "child_id",
      "b_child_name as child_name",
      "b_child_gender as child_gender",
      "b_child_cert_type as child_cert_type",
      "b_child_cert_no as child_cert_no",
      "b_child_birthday as child_birthday",
      "b_child_nationality as child_nationality",
      "child_policy_status",
      "child_start_date",
      "child_end_date",
      "child_create_time",
      "age"
    )
    result
  }


  //删除hdfs的文件，后输出
  def delete(master: String, path: String, tep_end: RDD[String]): Unit = {
    println("Begin delete!--" + master + path)
    val output = new org.apache.hadoop.fs.Path(master + path)
    val hdfs = org.apache.hadoop.fs.FileSystem.get(
      new java.net.URI(master), new org.apache.hadoop.conf.Configuration())
    // 删除输出目录
    if (hdfs.exists(output)) {
      hdfs.delete(output, true)
      println("delete!--" + master + path)
      tep_end.repartition(1).saveAsTextFile(path)
    } else tep_end.repartition(1).saveAsTextFile(path)

  }

  def main(args: Array[String]): Unit = {

    val lines: Iterator[String] = Source.fromURL(getClass.getResource("/config_scala.properties")).getLines

    val url: String = lines.filter(_.contains("location_mysql_url")).map(_.split("==")(1)).mkString("")

    val hdfs_url: String = lines.filter(_.contains("hdfs_url")).map(_.split("==")(1)).mkString("")

    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf_s = new SparkConf()
      .setAppName("baseinfo_merge")
//      .setMaster("local[4]")
      .set("spark.sql.broadcastTimeout", "36000")
      .set("spark.network.timeout", "36000")
      .set("spark.executor.heartbeatInterval","20000")
    val sc = new SparkContext(conf_s)

    val sqlContext: HiveContext = new HiveContext(sc)

    //创建uuid函数
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + ""))

    sqlContext.udf.register("getAge", (cert_no:String,end :String) => getAge(cert_no,end))

    val prop: Properties = new Properties

    //得到1.0表的数据 x
    val end_one = get_one(sqlContext: HiveContext, prop: Properties, url: String)
//    end_one.show(10)

    //得到2.0表的数据
    val end_two = get_two(sqlContext: HiveContext, prop: Properties, url: String)
//    end_two.show(10)
    //得到字段名字
    val fields_name = end_one.schema.map(x => x.name)
    val end_dataFrame = end_one.map(x => x).union(end_two.map(x => x))
//
//    //得到字段对应的值
    val field_value = end_dataFrame.map(x => {
      x.toSeq.map(x => if (x == null) "null" else x.toString).toArray
    })

    val schema_tepOne = StructType(fields_name.map(fieldName => StructField(fieldName, StringType, nullable = true)))
    //字段对应的值
    val value_tepTwo = field_value.map(r => Row(r: _*))
    //大表生成DF
    val big_before: DataFrame = sqlContext.createDataFrame(value_tepTwo, schema_tepOne)

//    println(big_before.count())
    //要过滤的数据
//    val odr_id = sqlContext.read.jdbc(url, "odr_policy", prop)
//      .where("insure_code!='1500001'")
//      .select("id").withColumnRenamed("id", "odr_id").cache
//
//    val big = big_before.join(odr_id, big_before("policy_id") === odr_id("odr_id"),"left").drop("odr_id")
//      .where("insured_name is not null").where("insured_name != 'null'")
    //ods_policy_insured_detail
    val table_name = "ods_policy_insured_detail_xing"
    big_before.insertInto(s"odsdb_prd.$table_name", overwrite = true) //存入哪张表中
    val tep_end: RDD[String] = big_before.map(_.mkString("mk6")).map(x => {
      val arrArray = x.split("mk6").map(x => if (x == "null" || x == null) "" else x)
      arrArray.mkString("mk6")
    }) //存入mysql
    delete(hdfs_url, "/oozie/mysqlData/ods_policy_insured_detail_xing", tep_end) //删除后，输出到文件中

    sc.stop()
  }
}
