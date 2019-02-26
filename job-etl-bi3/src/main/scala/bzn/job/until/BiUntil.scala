package bzn.job.until

import java.text.NumberFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FsShell, Path}
import org.apache.hadoop.hbase.client.{HTable, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat, LoadIncrementalHFiles, TableInputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext

object BiUntil {


  //得到企业标签数据
  def getHbase_value(sc: SparkContext): RDD[(ImmutableBytesWritable, Result)] = {
    //定义HBase的配置
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "172.16.11.106")

    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "labels:label_user_enterprise_vT")

    val usersRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    )
    usersRDD
  }

  //HBaseConf 配置
  def HbaseConf(tableName: String): (Configuration, Configuration) = {
    //定义HBase的配置
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "172.16.11.106")
    conf.set("mapreduce.task.timeout", "1200000")
    conf.set("hbase.client.scanner.timeout.period", "600000")
    conf.set("hbase.rpc.timeout", "600000")
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 3000)
    //设置配置文件，为了操作hdfs文件
    val conf_fs: Configuration = new Configuration()
    conf_fs.set("fs.default.name", "hdfs://namenode1.cdh:8020")
    (conf, conf_fs)
  }

  //企业风险::得到城市的编码
  def city_code(sqlContext: HiveContext): collection.Map[String, String] = {
    val d_city_grade = sqlContext.sql("select * from odsdb_prd.d_city_grade")
      .select("city_name", "city_code")
      .filter("length(city_name)>1 and city_code is not null")
    val d_city_grade_map: collection.Map[String, String] = d_city_grade.map(x => {
      val city_name = x.getAs("city_name").toString
      val city_code = x.getAs("city_code").toString
      (city_name, city_code)
    }).collectAsMap()
    d_city_grade_map
  }

  //企业风险::得到hbase标签
  def getHbase_label(tepOne: RDD[((ImmutableBytesWritable, Result), String, String)],
                     d_city_grade_map: collection.Map[String, String]): RDD[(Array[Double], String)] = {
    //遍历输出
    val numberFormat = NumberFormat.getInstance
    val vectors: RDD[(Array[Double], String)] = tepOne.map(x => {
      val work_one = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, s"worktype_1_count".getBytes))
      val work_one_res = if (work_one != null && x._3 != null) s"${numberFormat.format(work_one.toFloat / x._3.toFloat * 100)}%" else "0"

      val work_two = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, s"worktype_2_count".getBytes))
      val work_two_res = if (work_two != null && x._3 != null) s"${numberFormat.format(work_two.toFloat / x._3.toFloat * 100)}%" else "0"

      val work_three = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, s"worktype_3_count".getBytes))
      val work_three_res = if (work_three != null && x._3 != null) s"${numberFormat.format(work_three.toFloat / x._3.toFloat * 100)}%" else "0"

      val work_four = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, s"worktype_4_count".getBytes))
      val work_four_res = if (work_four != null && x._3 != null) s"${numberFormat.format(work_four.toFloat / x._3.toFloat * 100)}%" else "0"

      val work_five = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, s"worktype_5_count".getBytes))
      val work_five_res = if (work_five != null && x._3 != null) s"${numberFormat.format(work_five.toFloat / x._3.toFloat * 100)}%" else "0"

      //企业报案件数
      val report_num = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, "report_num".getBytes))
      val report_num_res = if (report_num != null) report_num else 0

      //死亡案件统计
      val death_num = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, "death_num".getBytes))
      val death_num_res = if (death_num != null) death_num else 0

      //伤残案件数
      val disability_num = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, "disability_num".getBytes))
      val disability_num_res = if (disability_num != null) disability_num else 0

      //已赚保费
      val charged_premium = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, "charged_premium".getBytes))
      val charged_premium_res = if (charged_premium != null) charged_premium.replaceAll(",", "") else "0"

      //实际已赔付金额
      val all_compensation = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, "all_compensation".getBytes))
      val all_compensation_res = if (all_compensation != null) all_compensation.replaceAll(",", "") else "0"

      //预估总赔付金额
      val pre_all_compensation = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, "pre_all_compensation".getBytes))
      val pre_all_compensation_res = if (pre_all_compensation != null) pre_all_compensation.replaceAll(",", "") else "0"

      //平均出险周期
      val avg_aging_risk_before = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, "avg_aging_risk".getBytes))
      val avg_aging_risk = if (avg_aging_risk_before != null) avg_aging_risk_before else "0"

      //企业的潜在人员规模
      val ent_potential_scale_before = Bytes.toString(x._1._2.getValue("baseinfo".getBytes, "ent_potential_scale".getBytes))
      val ent_potential_scale = if (ent_potential_scale_before != null) ent_potential_scale_before else "0"

      //员工平均年龄
      val ent_employee_age = Bytes.toString(x._1._2.getValue("baseinfo".getBytes, "ent_employee_age".getBytes))
      val ent_employee_age_res = if (ent_employee_age != null) ent_employee_age else "0"

      //城市
      val city_node = Bytes.toString(x._1._2.getValue("baseinfo".getBytes, "ent_city".getBytes))
      val ent_city = d_city_grade_map.getOrElse(city_node, "0")

      //当前在保人数
      val cur_insured_persons_before = Bytes.toString(x._1._2.getValue("insureinfo".getBytes, "cur_insured_persons".getBytes))
      val cur_insured_persons = if (cur_insured_persons_before != null) cur_insured_persons_before else "0"

      val work_level_array = Array(work_one_res, work_two_res, work_three_res, work_four_res, work_five_res)
      val work_type_number = work_level_array.zipWithIndex
      //哪个工种的占比最多，就找出对应的工种
      val work_type = work_type_number.map(x => {
        (x._1.replaceAll("%", "").replaceAll(",", "").toDouble, x._2 + 1)
      }).reduce((x1, x2) => {
        if (x1._1 >= x2._1) x1 else x2
      })._2 + ""

      //ent_id|rowKey
      val kv = x._1._2.getRow
      val rowkey = Bytes.toString(kv)
      val man = if (x._2 != null) x._2.split("-")(0).replaceAll("%", "") else "0"
      val woman = if (x._2 != null) x._2.split("-")(1).replaceAll("%", "") else "0"
      val ss = (s"哪个工种占比最多:$work_type\t当前在保:$cur_insured_persons\t男生占比:$man\t女生占比:$woman\t员工平均年龄:$ent_employee_age_res" +
        s"\t死亡案件数:$death_num_res\t伤残案件数:$disability_num_res" +
        s"\t报案数:$report_num_res\t平均出险周期:$avg_aging_risk" +
        s"\t企业的潜在人员规模:$ent_potential_scale\t$ent_city" +
        s"\t实际赔付额度:$all_compensation_res\t预估赔付额度:$pre_all_compensation_res\t已赚保费:$charged_premium_res", rowkey)

      (Array(work_type, cur_insured_persons, man, woman,
        ent_employee_age_res, death_num_res, disability_num_res,
        report_num_res, avg_aging_risk, ent_potential_scale, ent_city,
        all_compensation_res, pre_all_compensation_res, charged_premium_res).map(_.toString.toDouble), rowkey)
    })
    vectors
  }


  //对文件进行权限的设置
  def proessFile(conf_fs: Configuration, stagingFolder: String): Unit = {
    val shell = new FsShell(conf_fs)
    shell.run(Array[String]("-chmod", "-R", "777", stagingFolder))
  }

  //删除HFile文件
  def deleteFile(conf_fs: Configuration, stagingFolder: String): Unit = {
    val hdfs = FileSystem.get(conf_fs)
    val path = new Path(stagingFolder)
    hdfs.delete(path)
  }


  //将hfile存到Hbase中
  def toHbase(result: RDD[(String, String, String)], columnFamily1: String, column: String,
              conf_fs: Configuration, tableName: String, conf: Configuration) = {
    val stagingFolder = s"/oozie/hfile/$columnFamily1/$column"
    //创建hbase的链接,利用默认的配置文件,实际上读取的hbase的master地址
    val hdfs = FileSystem.get(conf_fs)
    val path = new Path(stagingFolder)

    //检查是否存在
    if (!hdfs.exists(path)) {
      //不存在就执行存储
      deToHbase()
    } else if (hdfs.exists(path)) {
      //存在即删除后执行存储
      deleteFile(conf_fs, stagingFolder)
      deToHbase()
    }

    def deToHbase() {
      val sourceRDD: RDD[(ImmutableBytesWritable, KeyValue)] = result
        .sortBy(_._1)
        .map(x => {
          //      val sourceRDD = result.map(x => {
          //rowkey
          val rowKey = Bytes.toBytes(x._1)
          //列族
          val family = Bytes.toBytes(columnFamily1)
          //字段
          val colum = Bytes.toBytes(x._3)
          //当前时间
          val date = new Date().getTime
          //数据
          val value = Bytes.toBytes(x._2)

          //将RDD转换成HFile需要的格式，并且我们要以ImmutableBytesWritable的实例为key
          (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
        })

      val table = new HTable(conf, tableName)
      lazy val job = Job.getInstance(conf)
      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setMapOutputValueClass(classOf[KeyValue])
      HFileOutputFormat.configureIncrementalLoad(job, table)
      sourceRDD.saveAsNewAPIHadoopFile(stagingFolder, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat], job.getConfiguration())

      //权限设置
      proessFile(conf_fs, stagingFolder + "/*")

      //开始导入
      val bulkLoader = new LoadIncrementalHFiles(conf)
      bulkLoader.doBulkLoad(new Path(stagingFolder), table)
    }
  }

}
