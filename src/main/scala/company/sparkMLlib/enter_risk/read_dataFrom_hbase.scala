package company.sparkMLlib.enter_risk

import java.text.NumberFormat

import company.hbase_label.until
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object read_dataFrom_hbase extends until {

  val conf_spark = new SparkConf()
    .setAppName("wuYu")
  conf_spark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf_spark.registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
  conf_spark.set("spark.sql.broadcastTimeout", "36000")
    .setMaster("local[2]")

  val sc = new SparkContext(conf_spark)

  val sqlContext: HiveContext = new HiveContext(sc)

  def main(args: Array[String]): Unit = {


    val dim_product = sqlContext.sql("select x.product_code from odsdb_prd.dim_product x where x.product_type_2='蓝领外包'").map(_.getAs("product_code").toString).collect()

    val bro = sc.broadcast(dim_product)


    val ods_policy_detail = sqlContext.sql("select ent_id,insure_code from odsdb_prd.ods_policy_detail").distinct()

    val ods_bro = ods_policy_detail.map(x => {
      val ent_id = x.getAs("ent_id").toString
      val insure_code = x.getAs("insure_code")
        .toString
      (ent_id, insure_code)
    }).filter(x => bro.value.contains(x._2))
      .map(_._1)
      .collect()

    val ods_bro_end = sc.broadcast(ods_bro)

    /**
      * 从Hbase中读取数据
      **/
    //    Spark读取HBase，我们主要使用SparkContext
    //    提供的newAPIHadoopRDDAPI将表的内容以 RDDs 的形式加载到 Spark 中。

    /**
      * 第一步:创建一个JobConf
      **/
    //定义HBase的配置
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "172.16.11.106")

    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "labels:label_user_enterprise_vT")

    val usersRDD = sc.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    )
    val count = usersRDD.count()
    println("copy_m RDD Count" + count)

    //遍历输出
    val numberFormat = NumberFormat.getInstance
    //    numberFormat.setMaximumFractionDigits(2)

    val result = usersRDD.map { x => {
      val s: (ImmutableBytesWritable, Result) = x
      val ent_man_woman_proportion = Bytes.toString(s._2.getValue("baseinfo".getBytes, "ent_man_woman_proportion".getBytes))
      val ent_scale = Bytes.toString(s._2.getValue("baseinfo".getBytes, "ent_scale".getBytes))
      val str = if (ent_man_woman_proportion == null) null else ent_man_woman_proportion.replaceAll(",", "-")
      val str_ent_scale = if (ent_scale == null) "0" else ent_scale
      //x|男女比例 | 总人数
      //      (x, str, str_ent_scale)
      (x, str, ent_scale)
    }
    }.map(x => {
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

      //将等级中是null的过滤掉
      //      val str_end = end.map(_._1).map(x => {
      //        val lab = x.split(":")(0)
      //        val value = x.split(":")(1)
      //        val end = if (value == "null") null else x
      //        end
      //      }).filter(_ != null).mkString(",")
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
      val charged_premium_res = if (charged_premium != null) charged_premium.replaceAll(",", "") else null

      //实际已赔付金额
      val all_compensation = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, "all_compensation".getBytes))
      val all_compensation_res = if (all_compensation != null) all_compensation.replaceAll(",", "") else null

      //实际已赔付金额 / 已赚保费
      val all_charge = if (all_compensation_res != null && charged_premium_res != null) s"${numberFormat.format(all_compensation_res.toFloat / charged_premium_res.toFloat * 100)}%" else null


      //预估总赔付金额
      val pre_all_compensation = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, "pre_all_compensation".getBytes))
      val pre_all_compensation_res = if (pre_all_compensation != null) pre_all_compensation.replaceAll(",", "") else "0"


      //预估总赔付金额 / 已赚保费
      val all_charge_pre = if (pre_all_compensation_res != null && charged_premium_res != null) s"${numberFormat.format(pre_all_compensation_res.toFloat / charged_premium_res.toFloat * 100)}%" else null


      //平均出险周期
      val avg_aging_risk = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, "avg_aging_risk".getBytes))

      //企业的死亡人数
      val ent_important_event_death = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, "ent_important_event_death".getBytes))
      val ent_important_event_death_res = if (ent_important_event_death != null) ent_important_event_death else 0


      //企业的伤残人数
      val ent_important_event_disable = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, "ent_important_event_disable".getBytes))
      val ent_important_event_disable_res = if (ent_important_event_disable != null) ent_important_event_disable else 0

      //企业的品牌影响力
      val ent_influence_level = Bytes.toString(x._1._2.getValue("baseinfo".getBytes, "ent_influence_level".getBytes))
      //企业的潜在人员规模
      val ent_potential_scale = Bytes.toString(x._1._2.getValue("baseinfo".getBytes, "ent_potential_scale".getBytes))

      val largecase_rate = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, "largecase_rate".getBytes))

      //工作期间案件数
      val worktime_num = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, "worktime_num".getBytes))
      val worktime_num_res = if (worktime_num != null) worktime_num else 0
      //非工作期间案件数
      val nonworktime_num = Bytes.toString(x._1._2.getValue("claiminfo".getBytes, "nonworktime_num".getBytes))
      val nonworktime_num_res = if (nonworktime_num != null) nonworktime_num else 0

      //员工平均年龄
      val ent_employee_age = Bytes.toString(x._1._2.getValue("baseinfo".getBytes, "ent_employee_age".getBytes))
      val ent_employee_age_res = if (ent_employee_age != null) ent_employee_age else "0"


      //城市
      val ent_city = Bytes.toString(x._1._2.getValue("baseinfo".getBytes, "ent_city".getBytes))

      //企业名称
      val ent_name = Bytes.toString(x._1._2.getValue("baseinfo".getBytes, "ent_name".getBytes))

      //当前在保人数
      val cur_insured_persons = Bytes.toString(x._1._2.getValue("insureinfo".getBytes, "cur_insured_persons".getBytes))

      //ent_id //rowKey
      val kv = x._1._2.getRow
      val rowkey = Bytes.toString(kv)
      //all_compensation_res：实际赔付
      //pre_all_compensation_res:预估
      //all_charge：实际/已赚
      //all_charge_pre：预估/已赚
      val man = if (x._2 != null) x._2.split("-")(0) else null
      val woman = if (x._2 != null) x._2.split("-")(1) else null
      val ss = (s"工种1:$work_one_res\t工种2:$work_two_res\t工种3:$work_three_res\t" +
        s"工种4:$work_four_res\t工种5:$work_five_res\t当前在保:$cur_insured_persons\t男生占比:$man\t女生占比:$woman\t员工平均年龄:$ent_employee_age_res" +
        s"\t死亡案件数:$death_num_res\t伤残案件数:$disability_num_res\t工作期间案件数:$worktime_num_res" +
        s"\t非工作案件数:$nonworktime_num_res\t报案数:$report_num_res\t平均出险周期:$avg_aging_risk" +
        s"\t重大案件率:$largecase_rate\t企业的潜在人员规模:$ent_potential_scale\t$ent_city" +
        s"\t$ent_name" +
        s"\t实际赔付额度:$all_compensation_res\t预估赔付额度:$pre_all_compensation_res\t已赚保费:$charged_premium_res\t实际/已赚:$all_charge\t预估/已赚:$all_charge_pre", rowkey)
      //      ss._1.split("\t").length
      ss
    })
      .filter(x => ods_bro_end.value.contains(x._2))
      .foreach(println(_))
    //      .repartition(1).saveAsTextFile("F:\\tmp\\company\\ents")
  }
}
