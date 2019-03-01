package bzn.job.label.ent_baseinfo

import java.text.NumberFormat

import bzn.job.until.EnterpriseUntil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object EntBaseinfo extends BaseinfoUntil with EnterpriseUntil {
  //12
  //企业的投保平均年龄(因为其有用到接口中的方法，因此无法放到接口中，只能放到该类中，防止序列化)
  def qy_avg(ods_policy_insured_detail: DataFrame,
             ods_policy_detail_table_T: DataFrame): RDD[(String, String, String)] = {
    // 创建一个数值格式化对象(对数字)
    val numberFormat = NumberFormat.getInstance
    // 设置精确到小数点后2位
    numberFormat.setMaximumFractionDigits(2)
    //计算该企业中:员工平均投保年龄(每个用户的投保日期-出生日期)求和后/企业的人数
    val ods_policy_insured_detail_table_age = ods_policy_insured_detail
      .filter("LENGTH(insured_start_date) > 1")
      .select("policy_id", "insured_birthday", "insured_start_date")
    val join_qy_mx_age = ods_policy_detail_table_T.join(ods_policy_insured_detail_table_age, "policy_id")
    //  | ent_id| policy_id|insured_birthday| insured_start_date|
    val end: RDD[(String, String, String)] = join_qy_mx_age
      .map(x => (x.getString(1), (x.getString(0), x.getString(2), x.getString(3))))
      .groupByKey
      .map(x => {
        val result = x._2
          .map(s => {
            (s._2, s._3)
          })
          .filter(x => if (x._1.contains("-") && x._2.contains("-")) true else false)

          .filter(f => {
            if (f._1.split("-")(0).toInt > 1900) true else false
          }).map(m => {
          val mp = m._2.split(" ")(0)
          val mp_one = m._1.split(" ")(0)

          (mp_one, m._2, year_tb_one(mp_one, mp).toInt)
        })
        val q_length = result.size.toDouble
        val sum_date = result.map(_._3).sum.toDouble
        val sq = sum_date / q_length
        (x._1, sq.toString, "ent_employee_age")
      })

    end
  }

  def BaseInfo(sqlContext: HiveContext) {
    //    val ods_policy_detail_table: DataFrame = sqlContext.sql("select policy_id,ent_id,user_id,policy_status from odsdb_prd.ods_policy_detail").cache()
    //现在使用的是ent_id，与ent_enterprise_info表的id关联，以前是user_id相关联的
    val ods_policy_detail_table: DataFrame = sqlContext.sql("select policy_id,ent_id,ent_id as user_id,policy_status from odsdb_prd.ods_policy_detail").cache()
    val ods_policy_detail_table_T: DataFrame = sqlContext.sql("select policy_id,ent_id from odsdb_prd.ods_policy_detail").filter("ent_id is not null").cache()
    val ods_policy_detail: DataFrame = sqlContext.sql("select * from odsdb_prd.ods_policy_detail").cache()

    val ent_enterprise_info = sqlContext.sql("select * from odsdb_prd.ent_enterprise_info").cache()
    val d_city_grade = sqlContext.sql("select * from odsdb_prd.d_city_grade").cache()
    val d_cant = sqlContext.sql("select * from odsdb_prd.d_cant").cache()
    val d_id_certificate = sqlContext.sql("select * from odsdb_prd.d_id_certificate").cache()
    val ods_policy_insured_detail = sqlContext.sql("select * from odsdb_prd.ods_policy_insured_detail")
    val ent_sum_level = sqlContext.sql("select * from odsdb_prd.ent_sum_level").cache()


    //HBaseConf
    val conf = HbaseConf("labels:label_user_enterprise_vT")._1
    val conf_fs = HbaseConf("labels:label_user_enterprise_vT")._2
    val tableName = "labels:label_user_enterprise_vT"
    val columnFamily1 = "baseinfo"
    //各表对应的信息:
    /**
      * ent_sum_level               企业等级信息表
      * ent_enterprise_info         官网企业信息表 功能是（官网企业信息）
      * d_id_certificate	          企业三证合一码表	企业三证合一信息
      * ods_policy_detail	          保单级别信息总表	保单级别综合数据
      * d_cant	                    省市区码表	省市区信息
      * d_city_grade	              城市级别码表	城市级别信息
      *
      * ods_policy_detail	          保单级别信息总表	保单级别综合数据
      * *   4abaa329-26e2-11e8-a3f3-1866daf7ab30    326fb42f111d4314a023a38b5d170e9e        O1712071702000000000000008005724        281867eb612f4c98adaa9b351e84d6d5        赵腾飞  18516396423     zhaotengfei@lifeuxuan.com        NULL    NULL    NULL    吕姗姗  2017-12-07 17:02:04     2017-12-07 17:02:04     073c7cb0191e432caa257445c381a3db        A1712460570     900000047702719243       众安在线财产保险股份有限公司    116c0b9fd91a45cfbff0d7ce80e41d0a        0       7100009 62700.12        2017-09-07 00:00:00     2018-09-06 23:59:59     2017-09-07 00:00:00     2017-09-06 00:00:00      1       -1      线下    2017-12-07 17:02:04     2018-03-08 17:07:22     8158f3e5784941f69d671112ad84eb69        1       上海瑟悟网络科技有限公司                zhaotengfei@lifeuxuan.com        18516396423                                                     上海瑟悟网络科技有限公司        上海市崇明县新村乡耀洲路741号5幢1029室  赵腾飞  18516396423      zhaotengfei@lifeuxuan.com       91310230MA1JXCPX8W      2017-12-07 17:02:04     2017-12-07 17:02:04                                                                             2101995cf10b4e5580e89824aa6e508c 保准牛众安骑士保30      保准牛众安骑士保30      众安在线财产保险股份有限公司    100801  雇主责任险      NULL    NULL    NULL    710000903       骑士保年缴10%30万435/年 435     30      1       2       2               0a789d56b7444d5197b1b3369876f24e        E0180821        上海瑟悟网络科技有限公司                91310230MA1JXCPX8W      0上海市崇明县新村乡耀洲路741号5幢1029室  NULL    NULL    赵腾飞          zhaotengfei@lifeuxuan.com       18516396423                             2
      *
      * ods_policy_insured_detail	  被保人清单级别信息总表	被保人明细级综合数据
      * *   56580f3e-26e2-11e8-a3f3-1866daf7ab30    000000fb59724b2dbc4d6e9b472dacf7        6d04d7669ad34e48873490303b0e3c54        2       1       文斌    1       1       110101198001138997      1980-01-13                       080304  电工(实习生)            邮件发送测试    17600920925     1       0               2017-08-01 14:51:01     2017-08-01 14:51:17     2017-08-04 00:00:00      2017-08-31 23:59:59
      **/

    //企业类型:列是(ent_type) [00ca1da523344782b34b9b63ec95913b,三证合一]
    val et = ent_type(ent_enterprise_info, d_id_certificate, ods_policy_detail_table).distinct()
    saveToHbase(et, columnFamily1, "ent_type", conf_fs, tableName, conf)

    //企业的注册时间:register_time [001eb1b2458940659345dd543245b86a,2017-07-13]
    val rt = ent_enterprise_info
      .select("id", "create_time")
      .map(x => (x.getString(0), x.getString(1).split(" ")(0), "register_time"))
      .distinct()
    saveToHbase(rt, columnFamily1, "register_time", conf_fs, tableName, conf)

    //企业名称：ent_name [001eb1b2458940659345dd543245b86a,青岛中企联人力资源开发有限公司]
    val en = ent_enterprise_info
      .select("id", "ent_name")
      .filter("length(ent_name)>0")
      .map(x => (x.getString(0), x.getString(1), "ent_name"))
      .distinct()
    saveToHbase(en, columnFamily1, "ent_name", conf_fs, tableName, conf)

    //企业所在省份：ent_province [69d42cd85a59444895210bb781919228,湖南省]
    val ep = ent_enterprise_info
      .select("id", "office_province")
      .where("id IS NOT NULL")
      .join(d_cant.select("name", "code"), ent_enterprise_info("office_province") === d_cant("code"))
      .map(x => (x.get(0) + "", x.get(2) + "", "ent_province"))
      .distinct()
    saveToHbase(ep, columnFamily1, "ent_province", conf_fs, tableName, conf)

    //企业所在城市：ent_city [309925dae0cd41f78dd25fd93a30e004,邯郸市]
    val ec = ent_enterprise_info
      .select("id", "office_city")
      .where("id IS NOT NULL")
      .join(d_cant.select("short_name", "code"), ent_enterprise_info("office_city") === d_cant("code"))
      .map(x => (x.get(0) + "", x.get(2) + "", "ent_city")).
      distinct()
    saveToHbase(ec, columnFamily1, "ent_city", conf_fs, tableName, conf)

    //该企业所在的城市类型：ent_city_type
    val ect = ent_enterprise_info
      .select("id", "office_city")
      .join(d_city_grade.select("city_grade", "city_code"), ent_enterprise_info("office_city") === d_city_grade("city_code"))
      .filter("office_city>1")
      .filter("city_code>1")
      .map(x => (x(0) + "", x(2) + "", "ent_city_type"))
      .distinct()
    saveToHbase(ect, columnFamily1, "ent_city_type", conf_fs, tableName, conf)

    //企业产品ID
    val qCp = qy_cp(ods_policy_detail: DataFrame).distinct
    saveToHbase(qCp, columnFamily1, "ent_insure_code", conf_fs, tableName, conf)

    //企业的男女比例
    val qy_sex_r = qy_sex(ods_policy_insured_detail, ods_policy_detail_table_T).distinct
    saveToHbase(qy_sex_r, columnFamily1, "ent_man_woman_proportion", conf_fs, tableName, conf)

    //企业平均投保年龄
    val qy_avg_r = qy_avg(ods_policy_insured_detail, ods_policy_detail_table_T).distinct
    saveToHbase(qy_avg_r, columnFamily1, "ent_employee_age", conf_fs, tableName, conf)

    //企业的人员规模
    val qy_gm_r = qy_gm(ent_sum_level).distinct
    saveToHbase(qy_gm_r, columnFamily1, "ent_scale", conf_fs, tableName, conf)

    //企业品牌影响力:end_id(企业id) ,ent_influence_level(企业的等级)
    val qy_pp_r = qy_pp(ent_sum_level).distinct
    saveToHbase(qy_pp_r, columnFamily1, "ent_influence_level", conf_fs, tableName, conf)

    //企业潜在人员规模
    val qy_qz_r = qy_qz(ods_policy_detail, ods_policy_insured_detail, ent_sum_level).distinct
    saveToHbase(qy_qz_r, columnFamily1, "ent_potential_scale", conf_fs, tableName, conf)
  }

  //列族是:baseinfo
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf_spark = new SparkConf()
      .setAppName("wuYu")
    conf_spark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf_spark.registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
    conf_spark.set("spark.sql.broadcastTimeout", "36000")
    //      .setMaster("local[2]")

    val sc = new SparkContext(conf_spark)
    val sqlContext: HiveContext = new HiveContext(sc)

    BaseInfo(sqlContext)
    sc.stop()
  }
}
