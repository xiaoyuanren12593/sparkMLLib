package bzn.job.label.goout

import bzn.job.common.Until
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SportGooutTest extends Until {


  def main(args: Array[String]): Unit = {

    val conf_s = new SparkConf().setAppName("wuYu")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
      .set("spark.sql.broadcastTimeout", "36000")
          .setMaster("local[2]")
    val sc = new SparkContext(conf_s)
    val sqlContext: HiveContext = new HiveContext(sc)

    val dim_product = sqlContext.sql("select * from odsdb_prd.dim_product")
    val ods_policy_insured_detail = sqlContext.sql("select * from odsdb_prd.ods_policy_insured_detail")

    val ods_policy_detail = sqlContext.sql("select * from odsdb_prd.ods_policy_detail")

    val total = dim_product
      .filter("product_type_1 = '体育'")
      .select("product_code", "product_name")
      .map(x => {
        val product_code = x.getString(0)
        val product_name = x.getString(1)

        val yM = bruteForceStringMatch(product_name, "羽毛球")
        val yRes = if (yM != -1) "羽毛球" else "-1"

        val yY = bruteForceStringMatch(product_name, "游泳")
        val yYRes = if (yY != -1) "游泳" else "-1"

        val xY = bruteForceStringMatch(product_name, "雪")
        val xYRes = if (xY != -1) "滑雪" else "-1"

        val tR = bruteForceStringMatch(product_name, "铁人三项")
        val tRRes = if (tR != -1) "铁人三项" else "-1"

        val aS = bruteForceStringMatch(product_name, "爱上水")
        val aSRes = if (aS != -1) "水上娱乐" else "-1"

        val sA = bruteForceStringMatch(product_name, "赛")
        val sARes = if (sA != -1) "赛事" else "-1"

        val mL = bruteForceStringMatch(product_name, "马拉松")
        val mLRes = if (mL != -1) "马拉松" else "-1"

        val pM = bruteForceStringMatch(product_name, "跑步")
        val pMRes = if (pM != -1) "马拉松" else "-1"

        val hB = bruteForceStringMatch(product_name, "滑冰")
        val hBRes = if (hB != -1) "滑冰" else "-1"

        val qL = bruteForceStringMatch(product_name, "球类")
        val qLRes = if (qL != -1) "球类" else "-1"

        (product_code, yRes, yYRes, xYRes, tRRes, aSRes, sARes, mLRes, pMRes, hBRes, qLRes)
      })

    //羽毛球（找出数据中的羽毛球）
    val yM = total.filter(_._2 != "-1").map(x => (x._1, (x._2, "user_badminton")))
    //游泳（找出数据中的游泳）
    val yY = total.filter(_._3 != "-1").map(x => (x._1, (x._3, "user_swimming")))
    //滑雪（找出数据中的滑雪）
    val hX = total.filter(_._4 != "-1").map(x => (x._1, (x._4, "user_skiing")))
    //铁人三项
    val tR = total.filter(_._5 != "-1").map(x => (x._1, (x._5, "user_triathlon")))
    //水上娱乐
    val sS = total.filter(_._6 != "-1").map(x => (x._1, (x._6, "user_water_entertainment")))
    //赛事
    val sSS = total.filter(_._7 != "-1").map(x => (x._1, (x._7, "user_competition")))
    //马拉松
    val mL = total
      .filter(_._8 != "-1")
      .map(x => (x._1, x._8))
      .union(total.filter(_._9 != "-1")
        .map(x => (x._1, x._9)))
      .map(x => (x._1, (x._2, "user_marathon")))
    //滑冰
    val hB = total.filter(_._10 != "-1").map(x => (x._1, (x._10, "user_skating")))
    //球类
    val qL = total.filter(_._11 != "-1").map(x => (x._1, (x._11, "user_footwear")))

    //找到保单号，保单ID，和每个人的身份证
    val tepThree = ods_policy_detail
      .where("policy_status in ('0','1','7','9','10')")
      .select("ent_id", "policy_id", "insure_code")
    val tepFour = ods_policy_insured_detail
      .filter("length(insured_cert_no)=18 and length(insured_create_time)>14")
      .select("policy_id", "insured_cert_no", "insured_create_time")
    val tepFive = tepThree.join(tepFour, "policy_id")
    //    |           policy_id|              ent_id|insure_code|   insured_cert_no|
    //    |cc1025c8708746dbb...|365104828040476c9...|    7100001|222402197008140614|
    val tepSix = tepFive.map(x => {
      val insure_code = x.getString(2)
      val insured_cert_no = x.getString(3)
      val insured_create_time = x.getAs("insured_create_time").toString.replaceAll("/", "-")
      (insure_code, s"$insured_cert_no\t$insured_create_time")
    })
    val tY = yM.union(yY).union(hX).union(tR).union(sS).union(sSS).union(mL).union(hB).union(qL)

    //跟上面的体育类项目通过保单号关联
    //身份证号 | 类型 | 存到HBase中的字段 | 保单创建时间(Long)类型
    val end = tepSix
      .join(tY)
      .map(x => {
        val data = x._2._1.split("\t")(1)
        (x._2._1.split("\t")(0), (x._2._2._1, x._2._2._2, currentTimeL(data)))
      })

    /**
      * HBaseConf
      **/
    val conf = HbaseConf("labels:label_user_personal_vT")._1
    val conf_fs = HbaseConf("labels:label_user_personal_vT")._2
    val tableName = "labels:label_user_personal_vT"
    val columnFamily1 = "goout"

    //羽毛球的找出来
    val user_badminton = end.filter(_._2._1 == "羽毛球").reduceByKey((x1, x2) => if (x1._3 >= x2._3) x1 else x2).map(x => (x._1, x._2._1, x._2._2))
//    toHbase(user_badminton, columnFamily1, "user_badminton", conf_fs, tableName, conf)
    user_badminton.take(10).foreach(x => println(x))

    //游泳
    val user_swimming = end.filter(_._2._1 == "游泳").reduceByKey((x1, x2) => if (x1._3 >= x2._3) x1 else x2).map(x => (x._1, x._2._1, x._2._2))
//    toHbase(user_swimming, columnFamily1, "user_swimming", conf_fs, tableName, conf)
    user_swimming.take(10).foreach(x => println(x))

    //滑雪
    val user_skiing = end.filter(_._2._1 == "滑雪").reduceByKey((x1, x2) => if (x1._3 >= x2._3) x1 else x2).map(x => (x._1, x._2._1, x._2._2))
//    toHbase(user_skiing, columnFamily1, "user_skiing", conf_fs, tableName, conf)
    user_skiing.take(10).foreach(x => println(x))

    //铁人三项
    val user_triathlon = end.filter(_._2._1 == "铁人三项").reduceByKey((x1, x2) => if (x1._3 >= x2._3) x1 else x2).map(x => (x._1, x._2._1, x._2._2))
//    toHbase(user_triathlon, columnFamily1, "user_triathlon", conf_fs, tableName, conf)
    user_triathlon.take(10).foreach(x => println(x))

    //水上娱乐
    val user_water_entertainment = end.filter(_._2._1 == "水上娱乐").reduceByKey((x1, x2) => if (x1._3 >= x2._3) x1 else x2).map(x => (x._1, x._2._1, x._2._2))
//    toHbase(user_water_entertainment, columnFamily1, "user_water_entertainment", conf_fs, tableName, conf)
    user_water_entertainment.take(10).foreach(x => println(x))

    //赛事
    val user_competition = end.filter(_._2._1 == "赛事").reduceByKey((x1, x2) => if (x1._3 >= x2._3) x1 else x2).map(x => (x._1, x._2._1, x._2._2))
//    toHbase(user_competition, columnFamily1, "user_competition", conf_fs, tableName, conf)
    user_competition.take(10).foreach(x => println(x))

    //马拉松
    val user_marathon = end.filter(_._2._1 == "马拉松").reduceByKey((x1, x2) => if (x1._3 >= x2._3) x1 else x2).map(x => (x._1, x._2._1, x._2._2))
//    toHbase(user_marathon, columnFamily1, "user_marathon", conf_fs, tableName, conf)
    user_marathon.take(10).foreach(x => println(x))

    //滑冰
    val user_skating = end.filter(_._2._1 == "滑冰").reduceByKey((x1, x2) => if (x1._3 >= x2._3) x1 else x2).map(x => (x._1, x._2._1, x._2._2))
//    toHbase(user_skating, columnFamily1, "user_skating", conf_fs, tableName, conf)
    user_skating.take(10).foreach(x => println(x))

    //球类
    val user_footwear = end.filter(_._2._1 == "球类").reduceByKey((x1, x2) => if (x1._3 >= x2._3) x1 else x2).map(x => (x._1, x._2._1, x._2._2))
//    toHbase(user_footwear, columnFamily1, "user_footwear", conf_fs, tableName, conf)
    user_footwear.take(10).foreach(x => println(x))

  }
}
