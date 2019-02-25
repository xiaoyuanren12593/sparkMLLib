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
  * Created by MK on 2018/7/5.
  */
object one_and_two_data_test extends year_until {
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
    val plc_policy_preserve = sqlContext.read.jdbc(url, "plc_policy_preserve", prop).withColumnRenamed("update_time", "a_update_time").withColumnRenamed("create_time", "a_create_time").withColumnRenamed("id", "a_id").withColumnRenamed("status", "a_status").withColumnRenamed("policy_id", "a_policy_id")
      .withColumnRenamed("policy_code", "a_policy_code").persist(StorageLevel.MEMORY_ONLY)
    sqlContext.read.jdbc(url, "plc_policy_preserve_insured", prop).where("remark != 'obsolete' or remark is null")
      .withColumnRenamed("status", "b_status").withColumnRenamed("create_time", "b_create_time")
      .withColumnRenamed("update_time", "b_update_time").withColumnRenamed("id", "b_id").registerTempTable("b")

    //将sql中的回车替换掉
    sqlContext.sql("select  regexp_replace(work_type,'\\n','') as work_type_new ,regexp_replace(cert_no,'\\n','') as cert_no_new,* from b")
      .drop("work_type").drop("cert_no")
      .withColumnRenamed("cert_no_new", "cert_no").withColumnRenamed("work_type_new", "work_type")
      .registerTempTable("b_new")

    val plc_policy_preserve_insured_version_one: DataFrame = sqlContext.sql("select work_type as work_type_new,* from b_new")
      .drop("work_type").withColumnRenamed("work_type_new", "work_type")

    val fields_mk: Seq[String] = plc_policy_preserve_insured_version_one.schema.map(x => x.name) :+ "work_type_mk" :+ "name_mk"
    val plc_policy_preserve_insured_version_two = plc_policy_preserve_insured_version_one.map(x => {
      val work = to_null(x.getAs[String]("work_type"))
      val name = to_null(x.getAs[String]("name"))

      (x.toSeq :+ work :+ name).map(x => if (x == null) "null" else x.toString)
    })
    val value = plc_policy_preserve_insured_version_two.map(r => Row(r: _*))
    val schema = StructType(fields_mk.map(fieldName => StructField(fieldName, StringType, nullable = true)))
    val plc_policy_preserve_insured_version_three = sqlContext.createDataFrame(value, schema)
    val plc_policy_preserve_insured =
      plc_policy_preserve_insured_version_three
        .withColumn("work_type", plc_policy_preserve_insured_version_three("work_type_mk"))
        .withColumn("name", plc_policy_preserve_insured_version_three("name_mk"))
        .drop("work_type_mk").drop("name_mk")


    val plc_policy_preserve_insured_child = sqlContext.read.jdbc(url, "plc_policy_preserve_insured_child", prop)
      .withColumnRenamed("id", "c_id").withColumnRenamed("create_time", "c_create_time")
      .withColumnRenamed("update_time", "c_update_time").persist(StorageLevel.MEMORY_ONLY)
    val b_policy = sqlContext.read.jdbc(url, "b_policy", prop).select("id", "insurance_policy_no").withColumnRenamed("id", "d_id").persist(StorageLevel.MEMORY_ONLY)

    val tep_one = plc_policy_preserve.join(plc_policy_preserve_insured, plc_policy_preserve("a_id") === plc_policy_preserve_insured("preserve_id"), "left")
    // .persist(StorageLevel.MEMORY_ONLY_SER)
    val tep_two = tep_one.join(plc_policy_preserve_insured_child, tep_one("b_id") === plc_policy_preserve_insured_child("insured_id"), "left")
    //.persist(StorageLevel.MEMORY_ONLY_SER)
    val tep_three = tep_two.join(b_policy, tep_two("a_policy_code") === b_policy("insurance_policy_no"), "left")

    val end: DataFrame = tep_three.selectExpr("" +
      "getUUID() as id",
      "a_id as preserve_id",
      "case when d_id is null then a_policy_id else d_id end policy_id",
      "a_policy_code as policy_code",
      "user_id",
      "a_status as preserve_status",
      "add_batch_code",
      "add_premium",
      "add_person_count",
      "del_batch_code",
      "del_premium",
      "del_person_count",
      "start_date as pre_start_date",
      "end_date as pre_end_date",
      "a_create_time as pre_create_time",
      "a_update_time as pre_update_time",
      "type as pre_type",
      "b_id as pre_insured_id",
      "is_legal as pre_is_legal",
      "name as insured_name",
      "gender as insured_gender",
      "cert_no as insured_cert_no",
      "birthday as insured_birthday",
      "profession as insured_profession",
      "industry as insured_industry",
      "work_type as insured_work_type", //25
      "nation as insured_nation",
      "company_name as insured_company_name",
      "company_phone as insured_company_phone",
      "is_chief_insurer",
      "changeType as insured_changeType",
      "join_date",
      "left_date",
      "b_status as insured_status",
      "occu_category",
      "b_create_time as insured_create_time",
      "b_update_time as insured_update_time",
      "c_id as child_id",
      "child_name",
      "child_gender",
      "child_cert_type",
      "child_cert_no",
      "child_birthday",
      "child_nationality",
      "child_join_date",
      "child_left_date",
      "change_type as child_change_type",
      "change_cause",
      "c_create_time as child_create_time",
      "c_update_time as child_update_time"
    )

    end
  }

  //表2.0数据
  def get_two(sqlContext: HiveContext, prop: Properties, url: String): DataFrame
  = {
    val b_policy_preservation_before = sqlContext.read.jdbc(url, "b_policy_preservation", prop)
      .withColumnRenamed("policy_no", "a_policy_no")
      .withColumnRenamed("id", "a_id")
      .withColumnRenamed("create_time", "a_create_time")
      .withColumnRenamed("update_time", "a_update_time")
      .withColumnRenamed("inc_dec_order_no", "a_inc_dec_order_no")
    val b_policy_preservation = b_policy_preservation_before
      .withColumn("mk_pol", b_policy_preservation_before("a_policy_no")) //新增mk_pol列
      .withColumn("mk_inc", b_policy_preservation_before("a_inc_dec_order_no"))
      .persist(StorageLevel.MEMORY_ONLY)

    val b_policy = sqlContext.read.jdbc(url, "b_policy", prop)
      .withColumnRenamed("policy_no", "b_policy_no")
      .withColumnRenamed("id", "b_id")
    //      .persist(StorageLevel.MEMORY_ONLY)

    val b_policy_preservation_subject_person_master_before = sqlContext.read.jdbc(url, "b_policy_preservation_subject_person_master", prop)
      .withColumnRenamed("policy_no", "c_policy_no")
      .withColumnRenamed("start_date", "c_start_date")
      .withColumnRenamed("end_date", "c_end_date")
      .withColumnRenamed("create_time", "c_create_time")
      .withColumnRenamed("update_time", "c_update_time")
      .withColumnRenamed("id", "c_id")
      .withColumnRenamed("industry_code", "c_industry_code")
      .withColumnRenamed("cert_no", "c_cert_no")
      .withColumnRenamed("birthday", "c_birthday")
      .withColumnRenamed("nation", "c_nation")
      .withColumnRenamed("company_phone", "c_company_phone")
      .withColumnRenamed("profession_code", "c_profession_code")
      .withColumnRenamed("sex", "c_sex")
      .withColumnRenamed("revise_status", "c_revise_status")
      .withColumnRenamed("is_legal", "c_is_legal")
      .withColumnRenamed("inc_dec_order_no", "c_inc_dec_order_no")
      .withColumnRenamed("company_name", "c_company_name")
      .withColumnRenamed("occu_category", "c_occu_category")
    val b_policy_preservation_subject_person_master_after = b_policy_preservation_subject_person_master_before
      .withColumn("mk_pol", b_policy_preservation_subject_person_master_before("c_policy_no"))
      .withColumn("mk_inc", b_policy_preservation_subject_person_master_before("c_inc_dec_order_no"))
    b_policy_preservation_subject_person_master_after.registerTempTable("mk_ce")

    val b_policy_preservation_subject_person_master_version_one: DataFrame = sqlContext.sql("select " +
      "*,CASE WHEN mk_ce.`status` = '1'" +
      "    THEN '0'" +
      "    ELSE '1'" +
      "  END as insured_status,work_type as c_work_type from mk_ce").drop("work_type") //.persist(StorageLevel.MEMORY_ONLY_SER)

    val fields_mk = b_policy_preservation_subject_person_master_version_one.schema.map(x => x.name) :+ "c_work_type_mk"
    val b_policy_preservation_subject_person_master_version_two = b_policy_preservation_subject_person_master_version_one.map(x => {
      val work = to_null(x.getAs[String]("c_work_type"))
      //     (x.toSeq :+ work ).map(_.toString)
      (x.toSeq :+ work).map(x => if (x == null) "null" else x.toString)
    })
    val value = b_policy_preservation_subject_person_master_version_two.map(r => Row(r: _*))
    val schema = StructType(fields_mk.map(fieldName => StructField(fieldName, StringType, nullable = true)))
    val b_policy_preservation_subject_person_master_version_three = sqlContext.createDataFrame(value, schema)
    val b_policy_preservation_subject_person_master = b_policy_preservation_subject_person_master_version_three.withColumn("c_work_type", b_policy_preservation_subject_person_master_version_three("c_work_type_mk")).drop("c_work_type_mk")


    val b_policy_preservation_subject_person_slave_before = sqlContext.read.jdbc(url, "b_policy_preservation_subject_person_slave", prop)
    val b_policy_preservation_subject_person_slave = b_policy_preservation_subject_person_slave_before
      .withColumn("mk_pol", b_policy_preservation_subject_person_slave_before("policy_no"))
      .withColumn("mk_inc", b_policy_preservation_subject_person_slave_before("inc_dec_order_no"))
      .withColumnRenamed("id", "d_id")
      .withColumnRenamed("name", "d_name")
      .withColumnRenamed("sex", "d_sex")
      .withColumnRenamed("cert_type", "child_cert_type")
      .withColumnRenamed("cert_no", "child_cert_no")
      .withColumnRenamed("birthday", "child_birthday")
      .withColumnRenamed("nation", "child_nationality")
      .withColumnRenamed("join_date", "child_join_date")
      .withColumnRenamed("left_date", "child_left_date")
      .withColumnRenamed("create_time", "child_create_time")
      .withColumnRenamed("update_time", "child_update_time")
      .persist(StorageLevel.MEMORY_ONLY)
    import sqlContext.implicits._
    val tep_one = b_policy_preservation.join(b_policy, b_policy_preservation("a_policy_no") === b_policy("b_policy_no"), "left")
    val tep_two = tep_one.join(b_policy_preservation_subject_person_master, Seq("mk_pol", "mk_inc"), "left")

    val tep_three = tep_two.join(b_policy_preservation_subject_person_slave, Seq("mk_pol", "mk_inc"), "left")
    tep_three.registerTempTable("end_one")

    //业务数据一条都不能少，因此将过滤掉的与非过滤掉的进行整合
    val tep_four = sqlContext.sql("select mk_inc,c_start_date,c_end_date from end_one").map(x => {
      val c_start_date = x.getAs[String]("c_start_date")
      val c_end_date = x.getAs[String]("c_end_date")
      val one = if (c_start_date == "null" || c_start_date == null) currentTimeL(c_end_date.toString.substring(0, 19)).toDouble else currentTimeL(c_start_date.toString.substring(0, 19)).toDouble
      val two = if (c_end_date == "null" || c_end_date == null) currentTimeL(c_start_date.toString.substring(0, 19)).toDouble else currentTimeL(c_end_date.toString.substring(0, 19)).toDouble
      (x.getAs[String]("mk_inc"), (one, two))
    }).reduceByKey((x1, x2) => {
      val one = if (x1._1 <= x2._1) x1._1 else x2._1
      val two = if (x1._2 >= x2._2) x1._2 else x2._2
      (one, two)
    }).map(x => {
      val one = get_current_date(x._2._1.toLong)
      val two = get_current_date(x._2._2.toLong)
      (x._1, one, two)
    })
      .toDF("mk_inc", "c_final_start_date", "c_final_end_date")
    //    //业务数据一条都不能少，因此将过滤掉的与非过滤掉的进行整合
    //    val tep_four = sqlContext.sql("select mk_inc,c_start_date,c_end_date from end_one").map(x => {
    //      val inc_dec_order_no = x.getAs[String]("mk_inc")
    //      val c_start_date = x.getAs[String]("c_start_date")
    //      val c_end_date = x.getAs[String]("c_end_date")
    //      val one = if (c_start_date == "null" || c_start_date == null) Double.PositiveInfinity else currentTimeL(c_start_date.toString.substring(0, 19))
    //      val two = if (c_end_date == "null" || c_end_date == null) 0 else currentTimeL(c_end_date.toString.substring(0, 19))
    //      (inc_dec_order_no, (one, two))
    //    }).reduceByKey((x1, x2) => {
    //      val one = if (x1._1 <= x2._1) x1._1 else x2._1
    //      val two = if (x1._2 >= x2._2) x1._2 else x2._2
    //      (one, two)
    //    }).map(x => {
    //      val one = if (x._2._1 == Double.PositiveInfinity) "null" else get_current_date(x._2._1.toLong)
    //      val two = if (x._2._2 == 0) "null" else get_current_date(x._2._2)
    //      (x._1, one, two)
    //    }).toDF("mk_inc", "c_final_start_date", "c_final_end_date") //.persist(StorageLevel.MEMORY_ONLY_SER)

    val end_final = tep_three.join(tep_four, "mk_inc")
    end_final.selectExpr("" +
      "getUUID() as id",
      "a_id as preserve_id",
      "b_id as policy_id",
      "insurance_policy_no as policy_code",
      "user_code as user_id",
      "'4' as preserve_status",
      "inc_revise_no as add_batch_code",
      "inc_revise_premium as add_premium",
      "inc_revise_sum as add_person_count",
      "dec_revise_no as del_batch_code",
      "dec_revise_premium as del_premium",
      "dec_revise_sum as del_person_count",
      "c_final_start_date as pre_start_date",
      "c_final_end_date as pre_end_date",
      "a_create_time as pre_create_time",
      "a_update_time as pre_update_time",
      "preservation_type as pre_type",
      "c_id as pre_insured_id",
      "c_is_legal as pre_is_legal",
      "name as insured_name",
      "c_sex as insured_gender",
      "c_cert_no as insured_cert_no",
      "c_birthday as insured_birthday",
      "c_profession_code as insured_profession",
      "c_industry_code as insured_industry",
      "c_work_type as insured_work_type",
      "c_nation as insured_nation",
      "c_company_name as insured_company_name",
      "c_company_phone as insured_company_phone",
      "'1' as is_chief_insurer",
      "c_revise_status as insured_changeType",
      "c_start_date as join_date",
      "c_end_date as left_date",
      "insured_status",
      "c_occu_category as occu_category",
      "c_create_time as insured_create_time",
      "c_update_time as insured_update_time",
      "d_id as child_id",
      "d_name as child_name",
      "d_sex as child_gender",
      "child_cert_type",
      "child_cert_no",
      "child_birthday",
      "child_nationality",
      "child_join_date",
      "child_left_date",
      "NULL AS child_changeType",
      "'' AS change_cause",
      "c_create_time as child_create_time",
      "c_update_time as child_update_time"
    )
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
    val conf_s = new SparkConf().setAppName("wuYu")
      .setMaster("local[4]")
    val sc = new SparkContext(conf_s)
    val sqlContext: HiveContext = new HiveContext(sc)
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))

    val prop: Properties = new Properties

    //得到1.0表的数据
    val end_one = get_one(sqlContext: HiveContext, prop: Properties, url: String)
//
    //得到2.0表的数据
    val end_two = get_two(sqlContext: HiveContext, prop: Properties, url: String)

    //得到字段名字
    val fields_name = end_one.schema.map(x => x.name)
    val end_dataFrame = end_one.map(x => x).union(end_two.map(x => x))


    //得到字段对应的值
    val field_value = end_dataFrame.map(x => {
      x.toSeq.map(x => if (x == null) "null" else x.toString).toArray
    })
    val schema_tepOne = StructType(fields_name.map(fieldName => StructField(fieldName, StringType, nullable = true)))
    //字段对应的值
    val value_tepTwo = field_value.map(r => Row(r: _*))
    //大表生成DF
    val big_before: DataFrame = sqlContext.createDataFrame(value_tepTwo, schema_tepOne)


    //要过滤的数据
    val odr_id = sqlContext.read.jdbc(url, "odr_policy", prop)
      .where("insure_code!='1500001'")
      .select("id").withColumnRenamed("id", "odr_id").cache

    val big = big_before.join(odr_id, big_before("policy_id") === odr_id("odr_id"),"left").drop("odr_id")
      .filter("insured_name is not null").filter("insured_name != 'null'")

    val table_name = "ods_policy_preserve_detail_vt"
////      sqlContext.sql("select * from odsdb_prd.ods_policy_preserve_detail limit 10").show()
    big.insertInto(s"odsdb_prd.$table_name", overwrite = true) //存入哪张表中
//    val tep_end: RDD[String] = big.map(_.mkString("mk6")).map(x => {
//      val arrArray = x.split("mk6").map(x => if (x == "null" || x == null) "" else x)
//      arrArray.mkString("mk6")
//    }) //存入mysql
//    delete(hdfs_url, "/oozie/mysqlData/ods_policy_preserve_detail", tep_end) //删除后，输出到文件中

  }
}
