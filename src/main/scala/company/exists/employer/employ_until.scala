package company.exists.employer

import java.io.StringReader

import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by MK on 2018/5/16.
  */
trait employ_until {
  //小表
  def little_table(sc: SparkContext, sQLContext: SQLContext, path: String): DataFrame
  = {
    //雇主小表
    val rdd: RDD[Array[String]] = sc.textFile(path).map(x => {
      val reader = new CSVReader(new StringReader(x.replaceAll("\"", "")))
      reader.readNext()
    })
    //取得列名,并将其作为字段名
    val schema = StructType(rdd.first.map(fieldName => StructField(fieldName, StringType, nullable = true)))
    //字段对应的值
    val value = rdd.map(r => Row(r: _*))
    //小表生成DF
    val little: DataFrame = sQLContext.createDataFrame(value, schema)
    little
  }

  //大表
  def big_table(sc: SparkContext, sQLContext: SQLContext, path_big: String): DataFrame
  = {
    //雇主大表
    val big_tepOne = sc.textFile(path_big).map(x => {
      val reader = new CSVReader(new StringReader(x.replaceAll("\"", "")))
      reader.readNext()
    })
    //取得列名,并将其作为字段名
    val schema_tepOne = StructType(big_tepOne.first.map(fieldName => StructField(fieldName, StringType, nullable = true)))
    //字段对应的值
    val value_tepTwo = big_tepOne.map(r => Row(r: _*))
    //大表生成DF
    val big = sQLContext.createDataFrame(value_tepTwo, schema_tepOne)
    big
  }


  //非雇主通过保单号进行匹配,包含匹配到的和非匹配到的
  def Policy_not_employer(tep_ones: DataFrame, tep_Two: DataFrame, bd: String, sQLContext: SQLContext): DataFrame
  = {
    val tep_one = tep_ones.groupBy("保单号").agg(("保费", "sum"), ("结算单金额", "sum")).withColumnRenamed("sum(保费)", "保费").withColumnRenamed("sum(结算单金额)", "结算单金额")

    val all = tep_Two.join(tep_one, bd)
    tep_Two.withColumn("bd_big", tep_Two(bd)).drop(tep_Two(bd)).registerTempTable("a")
    tep_one.withColumn("bd_little", tep_one(bd)).drop(tep_one(bd)).registerTempTable("b")
    all.registerTempTable("c")

    val machine = sQLContext.sql("select * , \"匹配成功\" as machine from c")
    val not_machine = sQLContext.sql("select \"无\"as 保单号 ,*,\"匹配失败\" as not_machine" +
      " from a left outer join b on a.bd_big=b.bd_little where b.bd_little is null")
    val result = not_machine.drop(not_machine("bd_little")).drop(not_machine("bd_big"))


    val end: DataFrame = machine.unionAll(
      result
    )
    end
  }


  //非雇主通过批单号进行匹配,包含匹配到的和非匹配到的
  def Batches_not_employer(tep_ones: DataFrame, tep_Two: DataFrame, pd: String, sQLContext: SQLContext): DataFrame
  = {
    val tep_one = tep_ones.groupBy("批单号").agg(("保费", "sum"), ("结算单金额", "sum")).withColumnRenamed("sum(保费)", "保费").withColumnRenamed("sum(结算单金额)", "结算单金额")

    val all = tep_Two.join(tep_one, pd)
    tep_Two.withColumn("pd_big", tep_Two(pd)).drop(tep_Two(pd)).registerTempTable("a")
    tep_one.withColumn("pd_little", tep_one(pd)).drop(tep_one(pd)).registerTempTable("b")
    all.registerTempTable("c")

    val machine = sQLContext.sql("select * , \"匹配成功\" as machine from c")
    val not_machine = sQLContext.sql("select \"无\"as 批单号 ,*,\"匹配失败\" as not_machine" +
      " from a left outer join b on a.pd_big=b.pd_little where b.pd_little is null")
    val result = not_machine.drop(not_machine("pd_little")).drop(not_machine("pd_big"))


    val end: DataFrame = machine.unionAll(
      result
    )
    end
  }


  //  查询A表中存在B表中不存在的值
  def Not_matching(sQLContext: SQLContext, tep_Two: DataFrame, tep_one: DataFrame): Unit
  = {
    //要查询a表存在但b表中不存在的信息，需要用not in查询，hive sql如下:
    tep_Two.registerTempTable("a")
    tep_one.registerTempTable("b")
    sQLContext.sql("select * from a left outer join b on a.保单号=b.保单号 where b.保单号 is null")

  }

  //雇主通过保单和批单进行匹配
  def employer_bd_pd(tep_Two_employer: DataFrame, tep_one_employer: DataFrame, all: DataFrame, sQLContext: SQLContext): DataFrame
  = {
    tep_Two_employer.withColumn("bd_big", tep_Two_employer("保单号")).withColumn("pd_big", tep_Two_employer("批单号")).drop(tep_Two_employer("保单号")).drop(tep_Two_employer("批单号"))
      .registerTempTable("a")
    tep_one_employer.withColumn("bd_little", tep_one_employer("保单号")).withColumn("pd_little", tep_one_employer("批单号")).drop(tep_one_employer("保单号")).drop(tep_one_employer("批单号"))
      .registerTempTable("b")

    all.registerTempTable("c")
    val machine = sQLContext.sql("select * , \"匹配成功\" as machine from c")

    val not_machine = sQLContext.sql("select \"无\"as 保单号,\"无\" as 批单号 ,*,\"匹配失败\" as not_machine" +
      " from a left outer join b on a.bd_big=b.bd_little and a.pd_big=b.pd_little where b.pd_little is null")
    val result = not_machine.drop(not_machine("pd_little")).drop(not_machine("pd_big")).drop(not_machine("bd_little")).drop(not_machine("bd_big"))
    val end: DataFrame = machine.unionAll(result)
    end
  }


  //非雇主-业务台账保存
  def not_employer_save(es: DataFrame, end: DataFrame, one: DataFrame, two: DataFrame): Unit = {

    es.repartition(1).write.format("com.databricks.spark.csv")
      .option("header", "true") //在csv第一行有属性"true"，没有就是"false"
      .option("delimiter", ",") //默认以","分割
      .save("C:\\Users\\a2589\\Desktop\\需求one\\非雇主-台账")


    end.repartition(1).write.format("com.databricks.spark.csv")
      .option("header", "true") //在csv第一行有属性"true"，没有就是"false"
      .option("delimiter", ",") //默认以","分割
      .save("C:\\Users\\a2589\\Desktop\\需求one\\雇主-蓝领外包")

    one.repartition(1).write.format("com.databricks.spark.csv")
      .option("header", "true") //在csv第一行有属性"true"，没有就是"false"
      .option("delimiter", ",") //默认以","分割
      .save("C:\\Users\\a2589\\Desktop\\需求one\\只非雇主")

    two.repartition(1).write.format("com.databricks.spark.csv")
      .option("header", "true") //在csv第一行有属性"true"，没有就是"false"
      .option("delimiter", ",") //默认以","分割
      .save("C:\\Users\\a2589\\Desktop\\需求one\\只雇主")


  }


  //非雇主-通过保单号和批单号进行匹配
  def not_employer(tep_Two: DataFrame, tep_one: DataFrame, sQLContext: SQLContext): DataFrame
  = {
    val all_one = tep_Two.join(tep_one, Seq("保单号", "批单号"))
    tep_Two.withColumn("bd_big", tep_Two("保单号")).withColumn("pd_big", tep_Two("批单号")).drop(tep_Two("保单号")).drop(tep_Two("批单号"))
      .registerTempTable("a")
    tep_one.withColumn("bd_little", tep_one("保单号")).withColumn("pd_little", tep_one("批单号")).drop(tep_one("保单号")).drop(tep_one("批单号"))
      .registerTempTable("b")

    all_one.registerTempTable("c")
    val machine = sQLContext.sql("select * , \"匹配成功\" as machine from c")
    val not_machine = sQLContext.sql("select \"无\"as 保单号,\"无\" as 批单号 ,*,\"匹配失败\" as not_machine" +
      " from a left outer join b on a.bd_big=b.bd_little and a.pd_big=b.pd_little where b.pd_little is null")
    val result = not_machine.drop(not_machine("pd_little")).drop(not_machine("pd_big")).drop(not_machine("bd_little")).drop(not_machine("bd_big"))
    val end: DataFrame = machine.unionAll(result)
    end
  }


  //  非雇主找到我小表中的数据，且该表的数据没有在大表中出现过
  def not_gu_little(tep_one: DataFrame, tep_Two: DataFrame, sQLContext: SQLContext): DataFrame
  = {
    tep_one.withColumn("bd_little", tep_one("保单号")).withColumn("pd_little", tep_one("批单号")).drop(tep_one("保单号")).drop(tep_one("批单号")).registerTempTable("a")
    tep_Two.withColumn("bd_big", tep_Two("保单号")).withColumn("pd_big", tep_Two("批单号")).drop(tep_Two("保单号")).drop(tep_Two("批单号")).registerTempTable("b")
    //找到我a表存在b表不存在的数据
    val end = sQLContext.sql("select *" +
      " from a left outer join b on a.bd_little=b.bd_big and a.pd_little=b.pd_big where b.pd_big is null")
    //      .withColumnRenamed("bd_little", "保单号").withColumnRenamed("pd_little", "批单号")
    val s = end.drop(end("bd_little")).drop(end("pd_little")).drop(end("bd_big")).drop(end("pd_big"))
    //      .select("保费", "结算单金额", "保单号", "批单号")
    s
  }

  //  雇主找到我小表中的数据，且该表的数据没有在大表中出现过
  def gu_little(tep_one_employer: DataFrame, tep_Two_employer: DataFrame, sQLContext: SQLContext): DataFrame
  = {
    tep_one_employer.withColumn("bd_little", tep_one_employer("保单号")).withColumn("pd_little", tep_one_employer("批单号")).drop(tep_one_employer("保单号")).drop(tep_one_employer("批单号")).registerTempTable("a")
    tep_Two_employer.withColumn("bd_big", tep_Two_employer("保单号")).withColumn("pd_big", tep_Two_employer("批单号")).drop(tep_Two_employer("保单号")).drop(tep_Two_employer("批单号")).registerTempTable("b")
    //找到我a表存在b表不存在的数据

    val end = sQLContext.sql("select *" +
      " from a left outer join b on a.bd_little=b.bd_big and a.pd_little=b.pd_big where b.pd_big is null")
    //      .withColumnRenamed("bd_little", "保单号").withColumnRenamed("pd_little", "批单号")
    //      .select("保费", "结算单金额", "保单号", "批单号")
    val s = end.drop(end("bd_little")).drop(end("pd_little")).drop(end("bd_big")).drop(end("pd_big"))

    s
  }

}
