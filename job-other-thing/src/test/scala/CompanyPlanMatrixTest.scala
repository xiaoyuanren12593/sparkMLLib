import java.sql.{Connection, Date, DriverManager}
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

import scala.io.Source
import scala.math.BigDecimal.RoundingMode

/**
  * author:xiaoYuanRen
  * Date:2019/8/21
  * Time:17:54
  * describe: this is new class
  **/
object CompanyPlanMatrixTest {
  def main (args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName(DiffrentHolderEqualPeson.getClass.getName)
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val sQLContext = new HiveContext(sc)

    import  sQLContext.implicits._

    val properties = getProPerties()
    val url = properties.getProperty("location_mysql_url_dwdb")
    val prop = new Properties()

    /**
      * 读取产品表
      */
    val dimpProduct = sQLContext.read.jdbc(url,"dim_product",prop)
      .selectExpr("product_code","dim_1")
      .where("dim_1 in ('外包雇主','骑士保','大货车')")
//        dimpProduct.show()

    /**
      * 读取保单表
      */
    val odsPolicyDetail = readMysqlTable(sQLContext,"ods_policy_detail")
      .selectExpr("policy_status","policy_id","insure_code","policy_code","trim(holder_company) as holder_company")
      .where("policy_status in ('0','1')")

    /**
      * 得到当前时间
      */
//    val date = new Date()
    /**
      * 读取当前在保人表
      */

    val odsPolicyCurrInsured = readMysqlTable(sQLContext,"ods_policy_curr_insured")
      .selectExpr("policy_id as policy_id_insured","curr_insured","day_id")
      .where("day_id = DATE_FORMAT(now(),'yyyyMMdd')")

    /**
      * 读取方案表
      */
    val odsPolicyProductPlan = readMysqlTable(sQLContext,"ods_policy_product_plan")
      .selectExpr("policy_code as policy_code_plan","sku_coverage","cast(case when sku_charge_type = '2' then (sku_price / 12) else sku_price end as decimal(13,4)) as sku_price")
    odsPolicyProductPlan.show(1000)

    /**
      * 人员明细数据
      */
    val odsPolicyInsuredDetail = readMysqlTable(sQLContext,"ods_policy_insured_detail")
      .selectExpr("policy_id as policy_id_insured_2","trim(insured_cert_no) as insured_cert_no")
//      .show()

    /**
      * 当前在保人
      */
    val insuredCountRes = odsPolicyDetail.join(odsPolicyInsuredDetail,odsPolicyDetail("policy_id")===odsPolicyInsuredDetail("policy_id_insured_2"))
      .selectExpr("policy_id_insured_2","insured_cert_no")
      .map(x => {
        val policy_id_insured_2 = x.getAs[String]("policy_id_insured_2")
        val insuredCertNo = x.getAs[String]("insured_cert_no")
        (policy_id_insured_2,insuredCertNo)
      })
      .groupByKey()
      .map(x => {
        var insuredCount = 0
        if(x._2 != null){
          insuredCount = x._2.toArray.distinct.length
        }
        (x._1,insuredCount)
      })
      .toDF("policy_id_insured_2","insured_count")

    /***
      * 读取渠道表
      */
    val odsEntGuzhuSalesMan = readMysqlTable(sQLContext,"ods_ent_guzhu_salesman")
      .selectExpr("trim(ent_name) as ent_name","channel_name","salesman","biz_operator","case when channel_name = '直客' then trim(ent_name) else trim(channel_name) end as channel_name_new")
//    odsEntGuzhuSalesMan.show()

    /***
      * 读取销售渠道
      */
    val odsEntSalesTeam = readMysqlTable(sQLContext,"ods_ent_sales_team")
      .selectExpr("case when team_name is null then '公司' else team_name end as team_name","sale_name")
//    odsEntSalesTeam.show()

    /***
      * 读取省份表
      */
    val odsPolicyProvince = readMysqlTable(sQLContext,"ods_policy_province")
      .selectExpr("policy_code as policy_code_province","office_province","office_city")
//    odsEntGuzhuSalesMan.show()

    /***
      * 读取城市表
      */
    val dictCant = readMysqlTable(sQLContext,"dict_cant")
      .selectExpr("code","short_name")
//    dictCant.show()

    /**
      * 预估赔付
      */
    val employerLiabilityClaims = readMysqlTable(sQLContext,"employer_liability_claims")
      .selectExpr("policy_no","cast(case when trim(final_payment) is null then trim(pre_com) else trim(final_payment) end as decimal(14,4)) as pre_com")
//    employerLiabilityClaims.show()

    /**
      * 已赚保费
      */
    val odsPolicyChargedMonth = readMysqlTable(sQLContext,"ods_policy_charged_month")
      .where("day_id <= DATE_FORMAT(now(),'yyyyMMdd')")
      .selectExpr("charged_premium","policy_id as policy_id_charge","day_id")
//      .map(x => {
//        val policy_id_charge = x.getAs[String]("policy_id_charge")
//        val charged_premium = x.getAs[java.]("charged_premium")
//      })

    /**
      * 读取工种级别表
      */
    val odsWorkGrade = readMysqlTable(sQLContext,"ods_work_grade")
      .selectExpr("policy_code as policy_code_work","profession_type")

    /**
      * 人员明细表和保单左连接
      */
    val insuredPolicyRes = odsPolicyCurrInsured.join(odsPolicyDetail,odsPolicyCurrInsured("policy_id_insured")===odsPolicyDetail("policy_id"),"leftouter")
      .selectExpr("policy_id","insure_code","policy_code","holder_company","policy_id_insured","curr_insured")

    val planInsuredPolicyRes = insuredPolicyRes.join(odsPolicyProductPlan,insuredPolicyRes("policy_code")===odsPolicyProductPlan("policy_code_plan"),"leftouter")
      .selectExpr("policy_id","insure_code","policy_code","holder_company","policy_id_insured","curr_insured","sku_coverage","sku_price")

    /**
      * 保单产品
      */
    val insuredProductRes = planInsuredPolicyRes.join(dimpProduct,planInsuredPolicyRes("insure_code")===dimpProduct("product_code"))
      .selectExpr("policy_id","policy_code","holder_company","policy_id_insured","curr_insured","sku_coverage","sku_price")
//    insuredProductRes.show()

    /**
      * 雇主渠道结果
      */
    val EntGuzhuInsuredProductRes = insuredProductRes.join(odsEntGuzhuSalesMan,insuredProductRes("holder_company")===odsEntGuzhuSalesMan("ent_name"),"leftouter")
      .selectExpr("policy_id","policy_code","holder_company","policy_id_insured","curr_insured","sku_coverage","sku_price","channel_name_new","biz_operator","salesman")
//    EntGuzhuInsuredProductRes.show()
//    EntGuzhuInsuredProductRes.printSchema()

    /***
      * 团队结果
      */
    val entGuzhuTeamInsuredProductRes = EntGuzhuInsuredProductRes.join(odsEntSalesTeam,EntGuzhuInsuredProductRes("salesman")===odsEntSalesTeam("sale_name"),"leftouter")
      .selectExpr("policy_id","policy_code","holder_company","policy_id_insured","curr_insured","sku_coverage","sku_price","channel_name_new","biz_operator","salesman","team_name")

    /**
      * 省份和城市
      */
    val provinceRes = odsPolicyProvince.join(dictCant,odsPolicyProvince("office_province")===dictCant("code"),"leftouter")
      .selectExpr("policy_code_province","office_city","short_name as province")

    val provinceCityRes = provinceRes.join(dictCant,odsPolicyProvince("office_city")===dictCant("code"),"leftouter")
      .selectExpr("policy_code_province","province","short_name as city")

    val proCityEntGuzhuTeamInsuredProductRes = entGuzhuTeamInsuredProductRes.join(provinceCityRes,entGuzhuTeamInsuredProductRes("policy_code")===provinceCityRes("policy_code_province"),"leftouter")
      .selectExpr("policy_id","policy_code","holder_company","policy_id_insured","curr_insured","sku_coverage","sku_price","channel_name_new","biz_operator","salesman","team_name","province","city")

//    proCityEntGuzhuTeamInsuredProductRes.printSchema()
    /***
      * 保费，预估赔付
      */
    val preProCityEntGuzhuTeamInsuredProductRes = proCityEntGuzhuTeamInsuredProductRes.join(employerLiabilityClaims,proCityEntGuzhuTeamInsuredProductRes("policy_code")===employerLiabilityClaims("policy_no"),"leftouter")
      .selectExpr("policy_id","policy_code","holder_company","policy_id_insured","curr_insured","sku_coverage","sku_price","channel_name_new",
        "biz_operator","salesman","team_name","province","city","pre_com")

    val chargePreProCityEntGuzhuTeamInsuredProductRes = preProCityEntGuzhuTeamInsuredProductRes.join(odsPolicyChargedMonth,preProCityEntGuzhuTeamInsuredProductRes("policy_id_insured")===odsPolicyChargedMonth("policy_id_charge"),"leftouter")
      .selectExpr("policy_id","policy_code","holder_company","policy_id_insured","curr_insured","sku_coverage","sku_price","channel_name_new",
        "biz_operator","salesman","team_name","province","city","pre_com","charged_premium")

    val resTemp = chargePreProCityEntGuzhuTeamInsuredProductRes.join(odsWorkGrade,chargePreProCityEntGuzhuTeamInsuredProductRes("policy_code")===odsWorkGrade("policy_code_work"),"leftouter")
      .selectExpr("policy_id","policy_code","holder_company","policy_id_insured","curr_insured","sku_coverage","sku_price","channel_name_new","biz_operator","salesman","team_name","province",
        "city","pre_com","charged_premium","profession_type")

    val resTempRes = resTemp.join(insuredCountRes,resTemp("policy_id_insured")===insuredCountRes("policy_id_insured_2"),"leftouter")
      .selectExpr("policy_id","policy_code","holder_company","policy_id_insured","curr_insured","sku_coverage","sku_price","channel_name_new","biz_operator","salesman","team_name","province",
        "city","pre_com","charged_premium","profession_type","insured_count")
      .where("channel_name_new is not null")

    val oneRes = resTempRes.selectExpr("channel_name_new","biz_operator","salesman","team_name","curr_insured","sku_coverage","province","city","pre_com","charged_premium","profession_type")
      .map(x => {
        val holderCompany = x.getAs[String]("channel_name_new")
        val salesman = x.getAs[String]("salesman")
        val teamName = x.getAs[String]("team_name")
        val bizOperator = x.getAs[String]("biz_operator")
        val province = x.getAs[String]("province")
        var city = x.getAs[String]("city")
        if(province == "北京市" || province == "天津市" || province == "上海市" || province == "重庆市"){
          city = province
        }
        val currInsured = x.getAs[Long]("curr_insured")
        var preCom = x.getAs[java.math.BigDecimal]("pre_com")
        if(preCom == null){
          preCom = java.math.BigDecimal.valueOf(0)
        }
        var chargedPremium = x.getAs[java.math.BigDecimal]("charged_premium")
        if(chargedPremium == null){
          chargedPremium = java.math.BigDecimal.valueOf(0)
        }
        val professionType = x.getAs[String]("profession_type")
        val skuCoverage = x.getAs[java.math.BigDecimal]("sku_coverage")
        var shortName = x.getAs[String]("channel_name_new")
        shortName = if(shortName.length > 5){
          shortName = shortName.substring(0,5)
          shortName
        }else{
          shortName
        }
        ((holderCompany,professionType,skuCoverage),(salesman,teamName,bizOperator,province,city,currInsured,preCom,chargedPremium,shortName))
      })
      .reduceByKey((x1,x2)=> {
        val salesman = x1._1
        val teamName = x1._2
        val bizOperator = x1._3
        val province = x1._4
        val city = x1._5
        val currInsured = x1._6+x2._6
        val preCom = x1._7.add(x2._7)
        val chargedPremium = x1._8.add(x2._8)
        val shortName = x1._9
        (salesman,teamName,bizOperator,province,city,currInsured,preCom,chargedPremium,shortName)
      })
      .map(x => {
        (x._1._1,x._2._1,x._2._2,x._2._3,x._2._4,x._2._5,x._2._6,x._2._7,x._2._8,x._1._2,x._1._3,x._2._9)
      })
      .toDF("holder_company","salesman","team_name","biz_operator","province","city","curr_insured","pre_com","charged_premium","profession_type","sku_coverage","short_name")

    val towRes = resTempRes.selectExpr("policy_id_insured","insured_count","sku_price","channel_name_new","sku_coverage","profession_type")
      .distinct()
      .map(x => {
        var currInsured = x.getAs[Int]("insured_count")
        if(currInsured == null){
          currInsured = 0
        }
        var skuPrice = x.getAs[java.math.BigDecimal]("sku_price")
        if(skuPrice == null){
          skuPrice = java.math.BigDecimal.valueOf(0)
        }
        val channelNameNew = x.getAs[String]("channel_name_new")
        val professionType = x.getAs[String]("profession_type")
        val skuCoverage = x.getAs[java.math.BigDecimal]("sku_coverage")
        val avgRes:Double = skuPrice.doubleValue()*currInsured
        ((channelNameNew,professionType,skuCoverage),(currInsured,avgRes,0.0))
      })
      .reduceByKey((x1,x2) => {
        val currInsured :Int= x2._1+x1._1
        val avgRes = x1._2+x2._2
        val res: Double = if(currInsured == 0 ) 0 else avgRes / currInsured
        (currInsured,avgRes,res)
      })
      .map(x => {
        (x._1._1,x._1._2,x._1._3,x._2._1,x._2._2,x._2._2/x._2._1)
      })
      .toDF("holder_company","profession_type","sku_coverage","insured_count","all_premium","avg_premium")

    val res = oneRes.join(towRes,Seq("holder_company","profession_type","sku_coverage"))
      .selectExpr("holder_company","salesman","team_name","biz_operator","province","city","curr_insured","cast(pre_com as decimal(14,4)) as pre_com",
        "cast(charged_premium as decimal(14,4)) as charged_premium","profession_type","cast(sku_coverage as decimal(14,0)) as sku_coverage","insured_count","all_premium",
        "cast(avg_premium as decimal(14,4)) as avg_premium","short_name")
    res.printSchema()
//    res.show()
//    saveASMysqlTable(res,"company_plan_matrix",SaveMode.Overwrite)
  }

  /**ss
    * 获取 Mysql 表的数据
    *
    * @param sqlContext
    * @param tableName 读取Mysql表的名字
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlTable(sqlContext: SQLContext, tableName: String): DataFrame = {
    val properties: Properties = getProPerties()
    sqlContext
      .read
      .format("jdbc")
      .option("url", properties.getProperty("mysql.url"))
      .option("driver", properties.getProperty("mysql.driver"))
      .option("user", properties.getProperty("mysql.username"))
      .option("password", properties.getProperty("mysql.password"))
      .option("numPartitions","10")
//      .option("partitionColumn","id")
//      .option("lowerBound", "0")
//      .option("upperBound","200")
      .option("dbtable", tableName)
      .load()
  }

  /**
    * 将DataFrame保存为Mysql表
    *
    * @param dataFrame 需要保存的dataFrame
    * @param tableName 保存的mysql 表名
    * @param saveMode  保存的模式 ：Append、Overwrite、ErrorIfExists、Ignore
    */
  def saveASMysqlTable(dataFrame: DataFrame, tableName: String, saveMode: SaveMode) = {
    var table = tableName
    val properties: Properties = getProPerties()
    val prop = new Properties //配置文件中的key 与 spark 中的 key 不同 所以 创建prop 按照spark 的格式 进行配置数据库
    prop.setProperty("user", properties.getProperty("mysql.username"))
    prop.setProperty("password", properties.getProperty("mysql.password"))
    prop.setProperty("driver", properties.getProperty("mysql.driver"))
    prop.setProperty("url", "jdbc:mysql://172.16.11.105:3306/tableau?useSSL=true&tinyInt1isBit=false&characterEncoding=utf8&autoReconnect=true&failOverReadOnly=false&rewriteBatchedStatements=true")
    if (saveMode == SaveMode.Overwrite) {
      var conn: Connection = null
      try {
        conn = DriverManager.getConnection(
          prop.getProperty("url"),
          prop.getProperty("user"),
          prop.getProperty("password")
        )
        val stmt = conn.createStatement
        table = table.toLowerCase
        stmt.execute(s"truncate table $table") //为了不删除表结构，先truncate 再Append
        conn.close()
      }
      catch {
        case e: Exception =>
          println("MySQL Error:")
          e.printStackTrace()
      }
    }
    dataFrame.write.mode(SaveMode.Append).jdbc(prop.getProperty("url"), table, prop)
  }

  /**
    * 获取配置文件
    *
    * @return
    */
  def getProPerties() = {
    val lines_source = Source.fromURL(getClass.getResource("/config_scala.properties")).getLines.toSeq
    var properties: Properties = new Properties()
    for (elem <- lines_source) {
      val split = elem.split("==")
      val key = split(0)
      val value = split(1)
      properties.setProperty(key,value)
    }
    properties
  }
}
