package bzn.job.etl

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import bzn.job.common.Until
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
import scala.math.BigDecimal.RoundingMode

/**
  * author:xiaoYuanRen
  * Date:2019/5/21
  * Time:9:47
  * describe: 1.0 系统保全表
  **/
object OdsPolicyDetail  extends Until {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val appName = this.getClass.getName

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    val oneRes = oneOdsPolicyDetail(hiveContext)
    val twores = twoOdsPolicyDetail(hiveContext)
    val res = oneRes.unionAll(twores)
    saveASMysqlTable(res,"ods_policy_detail",SaveMode.Overwrite)
//      .rdd.map(_.mkString("`"))
//      .map(x => {
//        val arrArray = x.split("`").map(x => if (x == "null" || x == null) "" else x)
//        arrArray.mkString("`")
//      })
//    val path = "/xing/data/OdsPolicyDetail"
//    delete(path,res)
    sc.stop()

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
    prop.setProperty("user", properties.getProperty("mysql.username.105"))
    prop.setProperty("password", properties.getProperty("mysql.password.105"))
    prop.setProperty("driver", properties.getProperty("mysql.driver"))
    prop.setProperty("url", properties.getProperty("mysql.url.105"))
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
    * 将保单明细表的初投保费和保全表的保费进行关联
    *
    * @param sqlContext
    * @param data1
    */
  def odsPolicyPayMoney(sqlContext: SQLContext, data1: DataFrame) = {
    import sqlContext.implicits._
    /**
      * 读取保全表信息,对保单进行分组，得到每个保单的总增员保费和总减员保费
      */
    val ods_preservation_detail =
      sqlContext.sql("select preserve_id,policy_id,add_premium,del_premium,preserve_status from odsdb.ods_preservation_detail")
        .where("preserve_status = '1'")
        .map(x => {
          val policyId = x.getAs[String]("policy_id")
          var addPremium = x.getAs[Double]("add_premium")
          var delPremium = x.getAs[Double]("del_premium")
          if (addPremium == null) {
            addPremium = 0.0
          }
          if (delPremium == null) {
            addPremium = 0.0
          }
          (policyId, (addPremium, delPremium))
        })
        .reduceByKey((x1, x2) => {
          var addPremium = x1._1 + x2._1
          var delPremium = x1._2 + x2._2
          (addPremium, delPremium)
        })
        .map(x => {
          val policyId = x._1
          val preservePremium = x._2._1 + x._2._1
          (policyId, preservePremium)
        })
        .toDF("policy_id_temp", "preservePremium")

    /**
      * 保单状态在保和保单终止的保单过期
      */
    val odsPolicyPremium = data1.select("policy_id", "sum_premium").where("policy_status in (0,1)")
      .join(ods_preservation_detail, data1("policy_id") === ods_preservation_detail("policy_id_temp"), "leftouter")
      .map(x => {
        val policyId = x.getAs[String]("policy_id")
        val sumPremium = x.getAs[Double]("sum_premium")
        val preservePremium = x.getAs[Double]("preservePremium")
        val resFirstPremium = sumPremium - preservePremium
        (policyId, resFirstPremium)
      })
      .toDF("policy_id_temp", "resFirstPremium")

    val res = data1.join(odsPolicyPremium, data1("policy_id") === odsPolicyPremium("policy_id_temp"), "leftouter")
      .selectExpr("id", "order_id", "order_code", "user_id", "product_code", "product_name", "policy_id", "policy_code",
        "case when resFirstPremium is not null and first_premium is null then resFirstPremium end as first_premium", "sum_premium",
        "holder_name", "insured_subject", "policy_start_date", "policy_end_date", "policy_status",
        "preserve_policy_no", "insure_company_name", "belongs_regional", "belongs_industry", "channel_id",
        "channel_name", "policy_create_time", "policy_update_time", "dw_create_time")
    res
  }

  /**
    * 2.0系统保单明细表
    *
    * @param sqlContext
    */
  def twoOdsPolicyDetail(sqlContext: HiveContext) = {
    import sqlContext.implicits._
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      //设置日期格式
      val date = df.format(new Date()) // new Date()为获取当前系统时间
      (date + "")
    })

    /**
      * 读取保单表
      */
    val bPolicyBzncen: DataFrame = readMysqlTable(sqlContext, "b_policy")
      .selectExpr("proposal_no as order_id","proposal_no as order_code","user_code as user_id", "premium_price as original_price","premium_price as price",
       "first_insure_premium as pay_amount","'' as sales_name","create_time as order_create_time","update_time as order_update_time",
        "id as policy_id","proposal_no as applicant_code","policy_no as master_policy_no","insurance_policy_no as policy_code","product_code as sku_id",
        "policy_type","product_code as insure_code", "product_code","sum_premium as premium","start_date", "end_date",
        "start_date as effect_date","proposal_time as order_date","case when status='1' and end_date>=now() then '1' else '0' end as policy_status",
        "sell_channel_code as channel_id","sell_channel_name as channel_name", "continued_policy_no", "insurance_name as insure_company_name",
        "insurance_name as manage_org_name","premium_price", "create_time as policy_create_time", "update_time as policy_update_time",
        "trim(holder_name) as holder_name", "premium_price as pdt_original_price","premium_price as pdt_current_price",
        "'' as minimum_premium","'' as sku_code","'' as sku_str","premium_price as sku_price","first_insure_master_num as number_of_pople","invoice_type")
      .cache()

    /**
      * 首先续投保单号不能为空，如果续投保单号存在，用保险公司保单号替换续投保单号，否则为空
      */
    val continuedPolicyNo = bPolicyBzncen.selectExpr("master_policy_no as policy_no", "policy_code as policy_code_slave")
      .where("policy_no is not null")
      .cache()

    /**
      * 读取投保人企业
      */
    val bPolicyHolderCompanyBzncen: DataFrame = readMysqlTable(sqlContext, "b_policy_holder_company")
      .selectExpr("policy_no", "contact_name","contact_tel as contact_mobile","contact_email","id as holder_id","'1' as holder_type","trim(name) as name",
        "'' as holder_gender","property as holder_cert_type","unite_credit_code as holder_cert_no","'' as holder_birthday","'' as holder_profession",
        "'' as holder_industry","'' as work_type","contact_email as holder_email","contact_tel as holder_mobile","'中国' as holder_nation",
        "province_code as holder_province","city_code as holder_city","county_code as holder_district","address as holder_street",
        "postal_code as holder_zipno","name as holder_company","license_address as holder_company_addr","contact_name as holder_contact_name",
        "contact_tel as holder_contact_mobile","contact_email as holder_contact_email","unite_credit_code as holder_org_code",
        "create_time as holder_create_time","update_time as holder_update_time")


    val bPolicyHolderCompanyUnion = bPolicyHolderCompanyBzncen

    /**
      * 读取产品表
      */
    val bsProductBzncen: DataFrame = readMysqlTable(sqlContext, "bs_product")
      .selectExpr("id as product_id","product_name","product_short_name as product_short_name","product_type as product_type_code",
        "product_code as product_code_2","'' as type_name")

    /**
      * 读取产品方案表
      */
    val bPolicyProductPlanBzncen = readMysqlTable(sqlContext, "b_policy_product_plan")
      .selectExpr("policy_no", "plan_amount", "contain_trainee", "payment_type", "injure_percent", "technology_fee", "brokerage_fee")
      .map(x => {
        val policyNo = x.getAs[String]("policy_no")
        val planAmount = x.getAs[Double]("plan_amount") //方案保额
        val containTrainee = x.getAs[String]("contain_trainee") //2.0 1 2 3 null 特约（是否包含实习生）
        val paymentType = x.getAs[Int]("payment_type") //类型 年缴月缴
        var injurePercent = x.getAs[Double]("injure_percent") //伤残比例
        var injurePercentres = ""
        if (injurePercent == 0.05) {
          injurePercentres = "1"
        } else if (injurePercent == 0.10) {
          injurePercentres = "2"
        } else {
          injurePercentres = null
        }

        (policyNo, (planAmount, containTrainee, injurePercentres, paymentType,""))
      })
      .reduceByKey((x1, x2) => {
        val res = x1
        res
      })
      .map(x => {
        (x._1, x._2._1, x._2._2, x._2._3, x._2._4,x._2._5)
      })
      .toDF("policy_no_plan", "sku_coverage", "sku_append", "sku_ratio", "sku_charge_type","sku_price_id")

    /**
      * 读取被保人企业
      */
    val bPolicySubjectCompanyBzncen: DataFrame = readMysqlTable(sqlContext, "b_policy_subject_company")
      .selectExpr("policy_no","id as insurant_id","case when relation='1' then '0' else '1' end as holder_relation","property as insurant_cert_type",
        "unite_credit_code as insurant_cert_no","province_code as insurant_province","city_code as insurant_city","name as insurant_company_name",
        "license_address as insurant_company_address","contact_name as insurant_contact_name","contact_tel as insurant_contact_mobile",
        "contact_email as insurant_contact_email","create_time as insurant_create_time","update_time as insurant_update_time")

    /**
      * 读取企业信息映射表
      */
    val odsPolicyEnterpriseMap: DataFrame = readMysqlTableNoId(sqlContext, "ods_policy_enterprise_map")
      .selectExpr("policy_code as policy_code_ent_map","ent_id as ent_id_map")
    /**
      * 读取企业信息表
      */
    val entEnterpriseInfo: DataFrame = readMysqlTable(sqlContext, "ent_enterprise_info")
      .selectExpr("id as ent_id","'' as ent_code","ent_name","license_code","org_code","tax_code","'' as join_social","office_address","office_province","office_city",
        "contact_name as ent_contact_name","'' as ent_contact_duty","contact_email as ent_contact_email","contact_mobile as ent_contact_mobile",
        "contact_telephone as ent_contact_telephone","'' as ent_type","biz_address as ent_biz_address","'' as ent_status","office_address as tax_address","contact_telephone as ent_telephone",
        "'' as ent_bank_name","'' as ent_bank_account")

    val entRes = odsPolicyEnterpriseMap.join(entEnterpriseInfo,odsPolicyEnterpriseMap("ent_id_map")===entEnterpriseInfo("ent_id"),"leftouter")
      .selectExpr("policy_code_ent_map","ent_id","ent_code","ent_name","license_code","org_code","tax_code","join_social","office_address","office_province",
        "office_city","ent_contact_name","ent_contact_duty","ent_contact_email","ent_contact_mobile","ent_contact_telephone","ent_type","ent_biz_address","ent_status",
        "tax_address","ent_telephone","ent_bank_name","ent_bank_account")

    val bPolicySubject = bPolicySubjectCompanyBzncen

    val bPolicyBzncenTemp = bPolicyBzncen.join(continuedPolicyNo, bPolicyBzncen("continued_policy_no") === continuedPolicyNo("policy_no"), "leftouter")
      .selectExpr("order_id","order_code","user_id", "original_price","price",
        "pay_amount","sales_name","order_create_time","order_update_time",
        "policy_id","applicant_code","master_policy_no","policy_code","sku_id",
        "policy_type","insure_code","product_code","premium","start_date", "end_date",
        "effect_date","order_date","policy_status",
        "channel_id","channel_name", "continued_policy_no",
        "insure_company_name","manage_org_name","premium_price", "policy_create_time", "policy_update_time","holder_name",
        "pdt_original_price","pdt_current_price","minimum_premium","sku_code","sku_str","sku_price","number_of_pople","invoice_type")

    /**
      * 保单表和投保人表进行关联
      */
    val bPolicyHolderCompany = bPolicyBzncenTemp.join(bPolicyHolderCompanyUnion, bPolicyBzncenTemp("master_policy_no") === bPolicyHolderCompanyUnion("policy_no"), "leftouter")
     .selectExpr("order_id","order_code","user_id", "original_price","price",
        "pay_amount","sales_name","order_create_time","order_update_time",
        "policy_id","applicant_code","master_policy_no","policy_code","sku_id",
        "policy_type","insure_code","product_code","premium","start_date", "end_date",
        "effect_date","order_date","policy_status",
        "channel_id","channel_name", "continued_policy_no",
        "insure_company_name","manage_org_name","premium_price", "policy_create_time", "policy_update_time","holder_name",
        "pdt_original_price","pdt_current_price","minimum_premium","sku_code","sku_str","sku_price",
        "contact_name","contact_mobile","contact_email","holder_id","holder_type","name",
        "holder_gender","holder_cert_type","holder_cert_no","holder_birthday","holder_profession",
        "holder_industry","work_type","holder_email","holder_mobile","holder_nation",
        "holder_province","holder_city","holder_district","holder_street",
        "holder_zipno","holder_company","holder_company_addr","holder_contact_name",
        "holder_contact_mobile","holder_contact_email","holder_org_code",
        "holder_create_time","holder_update_time","number_of_pople","invoice_type")
    /**
      * 上结果与产品表进行关联
      */
    val bPolicyHolderCompanyProductTemp = bPolicyHolderCompany.join(bsProductBzncen, bPolicyHolderCompany("product_code") === bsProductBzncen("product_code_2"), "leftouter")
      .selectExpr("order_id","order_code","user_id", "original_price","price",
        "pay_amount","sales_name","order_create_time","order_update_time",
        "policy_id","applicant_code","master_policy_no","policy_code","sku_id",
        "policy_type","insure_code","product_code","premium","start_date", "end_date",
        "effect_date","order_date","policy_status",
        "channel_id","channel_name", "continued_policy_no",
        "insure_company_name","manage_org_name","premium_price", "policy_create_time", "policy_update_time","holder_name",
        "pdt_original_price","pdt_current_price","minimum_premium","sku_code","sku_str","sku_price",
        "contact_name","contact_mobile","contact_email","holder_id","holder_type","name",
        "holder_gender","holder_cert_type","holder_cert_no","holder_birthday","holder_profession",
        "holder_industry","work_type","holder_email","holder_mobile","holder_nation",
        "holder_province","holder_city","holder_district","holder_street",
        "holder_zipno","holder_company","holder_company_addr","holder_contact_name",
        "holder_contact_mobile","holder_contact_email","holder_org_code",
        "holder_create_time","holder_update_time","product_id","product_name","product_short_name","product_type_code","type_name","number_of_pople","invoice_type")
    /**
      * 上述结果与产品方案表进行关联
      */
    val bPolicyHolderCompanyProduct = bPolicyHolderCompanyProductTemp.join(bPolicyProductPlanBzncen, bPolicyHolderCompanyProductTemp("master_policy_no") === bPolicyProductPlanBzncen("policy_no_plan"), "leftouter")
      .selectExpr("order_id","order_code","user_id", "original_price","price",
        "pay_amount","sales_name","order_create_time","order_update_time",
        "policy_id","applicant_code","master_policy_no","policy_code","sku_id",
        "policy_type","insure_code","product_code","premium","start_date", "end_date",
        "effect_date","order_date","policy_status",
        "channel_id","channel_name", "continued_policy_no",
        "insure_company_name","manage_org_name","premium_price", "policy_create_time", "policy_update_time","holder_name",
        "pdt_original_price","pdt_current_price","minimum_premium","sku_code","sku_str","sku_price",
        "contact_name","contact_mobile","contact_email","holder_id","holder_type","name",
        "holder_gender","holder_cert_type","holder_cert_no","holder_birthday","holder_profession",
        "holder_industry","work_type","holder_email","holder_mobile","holder_nation",
        "holder_province","holder_city","holder_district","holder_street",
        "holder_zipno","holder_company","holder_company_addr","holder_contact_name",
        "holder_contact_mobile","holder_contact_email","holder_org_code",
        "holder_create_time","holder_update_time","product_id","product_name","product_short_name","product_type_code","type_name",
        "sku_coverage", "sku_append", "sku_ratio", "sku_charge_type","sku_price_id","number_of_pople","invoice_type")

    /**
      * 与被保人信息表关联
      */
    val bPolicyHolderCompanyProductInsured = bPolicyHolderCompanyProduct.join(bPolicySubject, bPolicyHolderCompanyProduct("master_policy_no") === bPolicySubject("policy_no"), "leftouter")
       .selectExpr("order_id","order_code","user_id", "original_price","price",
        "pay_amount","sales_name","order_create_time","order_update_time",
        "policy_id","applicant_code","master_policy_no","policy_code","sku_id",
        "policy_type","insure_code", "product_code","premium","start_date", "end_date",
        "effect_date","order_date","policy_status",
        "channel_id","channel_name", "continued_policy_no",
        "insure_company_name","manage_org_name","premium_price", "policy_create_time", "policy_update_time","holder_name",
        "pdt_original_price","pdt_current_price","minimum_premium","sku_code","sku_str","sku_price",
        "contact_name","contact_mobile","contact_email","holder_id","holder_type","name",
        "holder_gender","holder_cert_type","holder_cert_no","holder_birthday","holder_profession",
        "holder_industry","work_type","holder_email","holder_mobile","holder_nation",
        "holder_province","holder_city","holder_district","holder_street",
        "holder_zipno","holder_company","holder_company_addr","holder_contact_name",
        "holder_contact_mobile","holder_contact_email","holder_org_code",
        "holder_create_time","holder_update_time","product_id","product_name","product_short_name","product_type_code","type_name",
        "sku_coverage", "sku_append", "sku_ratio", "sku_charge_type","sku_price_id","insurant_id",
        "holder_relation","insurant_cert_type",
        "insurant_cert_no","insurant_province","insurant_city","insurant_company_name",
        "insurant_company_address","insurant_contact_name","insurant_contact_mobile",
        "insurant_contact_email","insurant_create_time","insurant_update_time","number_of_pople","invoice_type")

    val bPolicyHolderCompanyProductNew =  bPolicyHolderCompanyProductInsured.join(entRes,bPolicyHolderCompanyProductInsured("policy_code")===entRes("policy_code_ent_map"),"leftouter")
      .selectExpr("order_id","order_code","user_id", "original_price","price",
        "pay_amount","sales_name","order_create_time","order_update_time",
        "policy_id","applicant_code","master_policy_no","policy_code","sku_id",
        "policy_type","insure_code","product_code","premium","start_date", "end_date",
        "effect_date","order_date","policy_status",
        "channel_id","channel_name", "continued_policy_no",
        "insure_company_name","manage_org_name","premium_price", "policy_create_time", "policy_update_time","holder_name",
        "pdt_original_price","pdt_current_price","minimum_premium","sku_code","sku_str","sku_price",
        "contact_name","contact_mobile","contact_email","holder_id","holder_type","name",
        "holder_gender","holder_cert_type","holder_cert_no","holder_birthday","holder_profession",
        "holder_industry","work_type","holder_email","holder_mobile","holder_nation",
        "holder_province","holder_city","holder_district","holder_street",
        "holder_zipno","holder_company","holder_company_addr","holder_contact_name",
        "holder_contact_mobile","holder_contact_email","holder_org_code",
        "holder_create_time","holder_update_time","insurant_id",
        "holder_relation","insurant_cert_type",
        "insurant_cert_no","insurant_province","insurant_city","insurant_company_name",
        "insurant_company_address","insurant_contact_name","insurant_contact_mobile",
        "insurant_contact_email","insurant_create_time","insurant_update_time",
        "product_id","product_name","product_short_name","product_type_code","type_name",
        "sku_coverage", "sku_append", "sku_ratio", "sku_charge_type","sku_price_id",
        "ent_id","ent_code","ent_name","license_code","org_code","tax_code","join_social","office_address","office_province",
        "office_city","ent_contact_name","ent_contact_duty","ent_contact_email","ent_contact_mobile","ent_contact_telephone","ent_type",
        "ent_biz_address","ent_status","tax_address","ent_telephone","ent_bank_name","ent_bank_account","number_of_pople","invoice_type","invoice_type")

    /**
      * 读取产品明细表,将蓝领外包以外的数据进行处理，用总保费替换初投保费
      */
    val odsProductDetail = sqlContext.sql("select product_code as product_code_slave,product_type_2 from odsdb_prd.dim_product")
      .where("product_type_2 <> '蓝领外包'")

    val resEnd =
      bPolicyHolderCompanyProductNew.join(odsProductDetail, bPolicyHolderCompanyProductNew("product_code") === odsProductDetail("product_code_slave"), "leftouter")
        .selectExpr("getUUID() as id","order_id","order_code","user_id","contact_name","contact_mobile","contact_email","original_price","price",
          "case when product_code_slave is not null and pay_amount is null then premium else pay_amount end as pay_amount","sales_name","order_create_time",
          "order_update_time",
          "policy_id","applicant_code","policy_code","insure_company_name","sku_id",
          "policy_type","insure_code","premium","start_date", "end_date",
          "effect_date","order_date","policy_status",
          "channel_id","channel_name",
          "policy_create_time", "policy_update_time","holder_id","holder_type",
          "case when name is null then holder_name else name end as holder_name",
          "case when holder_gender = '' then null else holder_gender end as holder_gender","holder_cert_type","holder_cert_no",
          "case when holder_birthday = '' then null else holder_birthday end as holder_birthday","holder_profession",
          "holder_industry","work_type","holder_email","holder_mobile","holder_nation",
          "holder_province","holder_city","holder_district","holder_street",
          "holder_zipno","holder_company","holder_company_addr","holder_contact_name",
          "holder_contact_mobile","holder_contact_email","holder_org_code",
          "holder_create_time","holder_update_time",
          "insurant_id",
          "holder_relation","insurant_cert_type",
          "insurant_cert_no","insurant_province","insurant_city","insurant_company_name",
          "insurant_company_address","insurant_contact_name","insurant_contact_mobile",
          "insurant_contact_email","insurant_create_time","insurant_update_time",
          "product_id","product_name","product_short_name","manage_org_name","product_type_code","type_name",
          "pdt_original_price","pdt_current_price","case when minimum_premium = '' then null else minimum_premium end as minimum_premium",
          "sku_code","sku_str","sku_price",
          "sku_coverage", "sku_append", "sku_charge_type", "sku_ratio","sku_price_id",
          "ent_id","ent_code","ent_name","license_code","org_code","tax_code","case when join_social = '' then null else join_social end as join_social",
          "office_address","office_province",
          "office_city","ent_contact_name","ent_contact_duty","ent_contact_email","ent_contact_mobile","ent_contact_telephone","ent_type",
          "ent_biz_address","case when ent_status = '' then null else ent_status end as ent_status","tax_address","ent_telephone",
          "ent_bank_name","ent_bank_account","number_of_pople","continued_policy_no as renewal_policy_code","'2.0' as system_source","invoice_type")

    /**
      * 读取初投保费表
      */
    val policyFirstPremiumBznprd: DataFrame = readMysqlTable(sqlContext, "policy_first_premium")
      .selectExpr("policy_id as policy_id_premium", "pay_amount as pay_amount_premium")

    val res = resEnd.join(policyFirstPremiumBznprd,resEnd("policy_id")===policyFirstPremiumBznprd("policy_id_premium"),"leftouter")
      .selectExpr("id","order_id","order_code","user_id","contact_name","contact_mobile","contact_email","original_price","price",
        "case when policy_id_premium is not null then pay_amount_premium else pay_amount end as pay_amount","sales_name","order_create_time",
        "order_update_time",
        "policy_id","applicant_code","policy_code","insure_company_name","sku_id",
        "policy_type","insure_code","premium","start_date", "end_date",
        "effect_date","order_date","policy_status",
        "channel_id","channel_name",
        "policy_create_time", "policy_update_time","holder_id","holder_type",
        "holder_name",
        "holder_gender","holder_cert_type","holder_cert_no",
        "holder_birthday","holder_profession",
        "holder_industry","work_type","holder_email","holder_mobile","holder_nation",
        "holder_province","holder_city","holder_district","holder_street",
        "holder_zipno","holder_company","holder_company_addr","holder_contact_name",
        "holder_contact_mobile","holder_contact_email","holder_org_code",
        "holder_create_time","holder_update_time",
        "insurant_id",
        "holder_relation","insurant_cert_type",
        "insurant_cert_no","insurant_province","insurant_city","insurant_company_name",
        "insurant_company_address","insurant_contact_name","insurant_contact_mobile",
        "insurant_contact_email","insurant_create_time","insurant_update_time",
        "product_id","product_name","product_short_name","manage_org_name","product_type_code","type_name",
        "pdt_original_price","pdt_current_price","minimum_premium",
        "sku_code","sku_str","sku_price",
        "sku_coverage", "sku_append", "sku_charge_type", "sku_ratio","sku_price_id",
        "ent_id","ent_code","ent_name","license_code","org_code","tax_code","join_social",
        "office_address","office_province",
        "office_city","ent_contact_name","ent_contact_duty","ent_contact_email","ent_contact_mobile","ent_contact_telephone","ent_type",
        "ent_biz_address","ent_status","tax_address","ent_telephone",
        "ent_bank_name","ent_bank_account","number_of_pople","renewal_policy_code","system_source","invoice_type")
    res
    //    bPolicyHolderCompanyProductNew.show()
  }

  /**
    * 1.0系统保单明细表
    *
    * @param sqlContext
    */
  def oneOdsPolicyDetail(sqlContext: HiveContext) = {
    import sqlContext.implicits._
    sqlContext.udf.register("getUUID", () => (java.util.UUID.randomUUID() + "").replace("-", ""))
    sqlContext.udf.register("getNow", () => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      //设置日期格式
      val date = df.format(new Date()) // new Date()为获取当前系统时间
      (date + "")
    })

    /**
      * 读取订单信息表
      */
    val odrOrderInfoBznprd: DataFrame = readMysqlTable(sqlContext, "odr_order_info")
      .selectExpr("id as master_order_id", "order_code", "user_id", "pay_amount as pay_amount_master","contact_name","contact_mobile","contact_email","original_price",
      "price","sales_name","create_time as order_create_time","update_time as order_update_time")
//            odrOrderInfoBznprd.show()

    /**
      * 读取初投保费表
      */
    val policyFirstPremiumBznprd: DataFrame = readMysqlTable(sqlContext, "policy_first_premium")
      .selectExpr("policy_id as policy_id_premium", "pay_amount")

    /**
      * 读取1.0保单信息
      */
    val odrPolicyBznprd: DataFrame = readMysqlTable(sqlContext, "odr_policy")
      .selectExpr("id as master_policy_id", "applicant_code", "policy_code","insure_company_name", "order_id","policy_type","insure_code", "premium", "status",
        "start_date", "end_date", "effect_date","renewal_policy_code","order_date","channelId", "channel_name", "create_time as policy_create_time",
        "update_time as policy_update_time","invoice_type")
//            odrPolicyBznprd.show()

    /**
      * 读取投保人信息表
      */
    val odrPolicyHolderBznprd: DataFrame = readMysqlTable(sqlContext, "odr_policy_holder")
      .selectExpr("policy_id", "id as holder_id","holder_type","name as holder_name", "gender as holder_gender","cert_type as holder_cert_type","cert_no as holder_cert_no","birthday as holder_birthday",
        "profession as holder_profession","industry as holder_industry","work_type","email as holder_email","mobile as holder_mobile","nation as holder_nation","province as holder_province",
        "city as holder_city", "district as holder_district","street as holder_street","zip_no as holder_zipno","company_name as holder_company","company_address as holder_company_addr",
        "contact_name as holder_contact_name","contact_mobile as holder_contact_mobile","contact_email as holder_contact_email","org_code as holder_org_code","create_time as holder_create_time",
        "update_time as holder_update_time")
//        odrPolicyHolderBznprd.show()

    /**
      * 读取被保企业信息表
      */
    val odrPolicyInsurantBznprd: DataFrame = readMysqlTable(sqlContext, "odr_policy_insurant")
      .selectExpr("id as insurant_id","policy_id", "holder_relation","cert_type as insurant_cert_type","cert_no as insurant_cert_no","province as insurant_province","city as insurant_city",
        "company_name as insurant_company_name","company_address as insurant_company_address","contact_name as insurant_contact_name","contact_mobile as insurant_contact_mobile",
        "contact_email as insurant_contact_email","create_time as insurant_create_time","update_time as insurant_update_time")
//        odrPolicyInsurantBznprd.show()

    /**
      * 读取产品表
      */
    val pdtProductBznprd: DataFrame = readMysqlTable(sqlContext, "pdt_product")
      .selectExpr("id as product_id","name as product_name","short_name as product_short_name","manage_org_name","type_code as product_type_code","type_name",
        "original_price as pdt_original_price","current_price as pdt_current_price","code as product_code")
//        pdtProductBznprd.show()

    /**
      * 读取子保单表
      */
    val odrOrderItemInfoBznprd: DataFrame = readMysqlTable(sqlContext, "odr_order_item_info")
      .selectExpr("order_id", "industry_code", "quantity")
//        odrOrderItemInfoBznprd.show()

    /**
      * 读取保单表和方案表作为临时表
      */
    val odrPolicyBznprdTemp = readMysqlTable(sqlContext, "odr_policy")
      .selectExpr("id", "sku_id")
    /**
      * 读取产品方案表
      */
    val pdtProductSkuBznprd: DataFrame = readMysqlTable(sqlContext, "pdt_product_sku")
      .selectExpr("id as sku_id_slave", "minimum_premium","sku_code","sku_str","term_one", "term_three","price","price_id as sku_price_id")

    /**
      * 从产品方案表中获取保费，特约，保费类型，伤残赔付比例
      */
    val policyRes = odrPolicyBznprdTemp.join(pdtProductSkuBznprd, odrPolicyBznprdTemp("sku_id") === pdtProductSkuBznprd("sku_id_slave"), "leftouter")
      .selectExpr("id as policy_id_sku","minimum_premium","sku_code","sku_str","sku_id", "term_one", "term_three", "price","sku_price_id")
      .map(x => {
        val policyIdSku = x.getAs[String]("policy_id_sku")
        var minimumPremium = x.getAs[java.math.BigDecimal]("minimum_premium")
        val skuCode = x.getAs[String]("sku_code")
        val skuStr = x.getAs[String]("sku_str")
        val skuId = x.getAs[String]("sku_id")
        val skuPriceId = x.getAs[String]("sku_price_id")
        val termOne = x.getAs[Int]("term_one")
        val termThree = x.getAs[Int]("term_three")
        var price: java.math.BigDecimal = x.getAs[java.math.BigDecimal]("price")
        if (price != null) {
          price = price.setScale(4, RoundingMode.HALF_UP).bigDecimal
        }

        if(minimumPremium != null){
          minimumPremium = minimumPremium.setScale(4, RoundingMode.HALF_UP).bigDecimal
        }

        var skuCoverage = "" //保费
        var skuAppend = "" //特约
        var sku_charge_type = "" //保费类型  年缴或者月缴
        var sku_ratio = "" //伤残赔付比例
        if (termThree != null && termThree.toString.length == 5) {
          skuCoverage = termThree.toString.substring(0, 2)
        } else {
          if (termOne != null) {
            skuCoverage = termOne.toString
          } else {
            skuCoverage = null
          }
        }

        if (termThree != null && termThree.toString.length == 5) {
          skuAppend = termThree.toString.substring(2, 3)
        } else {
          skuAppend = null
        }

        if (termThree != null && termThree.toString.length == 5) {
          sku_charge_type = termThree.toString.substring(3, 4)
        } else {
          sku_charge_type = null
        }

        if (termThree != null && termThree.toString.length == 5) {
          sku_ratio = termThree.toString.substring(4, 5)
        } else {
          sku_ratio = null
        }

        (policyIdSku, minimumPremium,skuCode,skuStr,skuId, skuCoverage, skuAppend, sku_ratio, price, sku_charge_type,skuPriceId)
      })
      .toDF("policy_id_sku", "minimum_premium","sku_code","sku_str","sku_id", "sku_coverage", "sku_append", "sku_ratio", "sku_price", "sku_charge_type","sku_price_id")
      .distinct()


    /**
      * 读取企业信息映射表
      */
    val odsPolicyEnterpriseMap: DataFrame = readMysqlTableNoId(sqlContext, "ods_policy_enterprise_map")
      .selectExpr("policy_code as policy_code_ent_map","ent_id as ent_id_map")
    /**
      * 读取企业信息表
      */
    val entEnterpriseInfo: DataFrame = readMysqlTable(sqlContext, "ent_enterprise_info")
      .selectExpr("id as ent_id","'' as ent_code","ent_name","license_code","org_code","tax_code","'' as join_social","office_address","office_province","office_city",
      "contact_name as ent_contact_name","'' as ent_contact_duty","contact_email as ent_contact_email","contact_mobile as ent_contact_mobile",
      "contact_telephone as ent_contact_telephone","'' as ent_type","biz_address as ent_biz_address","'' as ent_status","office_address as tax_address","contact_telephone as ent_telephone",
      "'' as ent_bank_name","'' as ent_bank_account")

    val entRes = odsPolicyEnterpriseMap.join(entEnterpriseInfo,odsPolicyEnterpriseMap("ent_id_map")===entEnterpriseInfo("ent_id"),"leftouter")
      .selectExpr("policy_code_ent_map","ent_id","ent_code","ent_name","license_code","org_code","tax_code","join_social","office_address","office_province",
        "office_city","ent_contact_name","ent_contact_duty","ent_contact_email","ent_contact_mobile","ent_contact_telephone","ent_type","ent_biz_address","ent_status",
        "tax_address","ent_telephone","ent_bank_name","ent_bank_account")

    val orderPolicyTemp = odrOrderInfoBznprd.join(odrPolicyBznprd, odrOrderInfoBznprd("master_order_id") === odrPolicyBznprd("order_id"), "leftouter")
      .selectExpr("master_order_id", "order_code", "user_id", "pay_amount_master","contact_name","contact_mobile","contact_email","original_price",
      "price","sales_name","order_create_time","order_update_time","master_policy_id", "applicant_code", "policy_code","insure_company_name",
        "order_id","policy_type","insure_code", "premium", "status","start_date", "end_date", "effect_date","renewal_policy_code","order_date","channelId",
        "channel_name", "policy_create_time", "policy_update_time","invoice_type")

    val orderPolicy = orderPolicyTemp.join(policyFirstPremiumBznprd, orderPolicyTemp("master_policy_id") === policyFirstPremiumBznprd("policy_id_premium"), "leftouter")
      .selectExpr("master_order_id", "order_code", "user_id", "case when policy_id_premium is not null then pay_amount else pay_amount_master end as pay_amount",
        "contact_name","contact_mobile","contact_email","original_price","price","sales_name","order_create_time","order_update_time","master_policy_id", "applicant_code",
        "policy_code","insure_company_name","order_id","policy_type","insure_code", "premium", "status","start_date", "end_date", "effect_date","renewal_policy_code",
        "order_date","channelId","channel_name", "policy_create_time", "policy_update_time","invoice_type")

    val orderPolicyProductTemp = orderPolicy.join(pdtProductBznprd, orderPolicy("insure_code") === pdtProductBznprd("product_code"), "leftouter")
      .selectExpr("master_order_id", "order_code", "user_id", "pay_amount",
        "contact_name","contact_mobile","contact_email","original_price","price","sales_name","order_create_time","order_update_time","master_policy_id", "applicant_code",
        "policy_code","insure_company_name","order_id","policy_type","insure_code", "premium", "status","start_date", "end_date", "effect_date","renewal_policy_code",
        "order_date","channelId","channel_name", "policy_create_time", "policy_update_time","product_id","product_name","product_short_name",
        "manage_org_name","product_type_code","type_name","pdt_original_price","pdt_current_price","invoice_type")

    val orderPolicyProduct = orderPolicyProductTemp.join(policyRes, orderPolicyProductTemp("master_policy_id") === policyRes("policy_id_sku"), "leftouter")
      .selectExpr("master_order_id", "order_code", "user_id", "pay_amount",
        "contact_name","contact_mobile","contact_email","original_price","price","sales_name","order_create_time","order_update_time","master_policy_id", "applicant_code",
        "policy_code","insure_company_name","sku_id","policy_type","insure_code", "premium", "status","start_date", "end_date", "effect_date","renewal_policy_code",
        "order_date","channelId","channel_name", "policy_create_time", "policy_update_time","product_id","product_name","product_short_name",
        "manage_org_name","product_type_code","type_name","pdt_original_price","pdt_current_price","minimum_premium","sku_code","sku_str","sku_id", "sku_coverage",
        "sku_append", "sku_ratio", "sku_price", "sku_price_id","sku_charge_type","invoice_type")


    val orderPolicyProductHolder = orderPolicyProduct.join(odrPolicyHolderBznprd, orderPolicyProduct("master_policy_id") === odrPolicyHolderBznprd("policy_id"), "leftouter")
      .selectExpr("master_order_id", "order_code", "user_id", "pay_amount",
        "contact_name","contact_mobile","contact_email","original_price","price","sales_name","order_create_time","order_update_time","master_policy_id", "applicant_code",
        "policy_code","insure_company_name","sku_id","policy_type","insure_code", "premium", "status","start_date", "end_date", "effect_date","renewal_policy_code",
        "case when order_date is null then order_create_time else order_date end as order_date","channelId","channel_name", "policy_create_time", "policy_update_time",
        "holder_id","holder_type","holder_name", "holder_gender", "holder_cert_type","holder_cert_no","holder_birthday","holder_profession","holder_industry",
        "work_type","holder_email","holder_mobile","holder_nation", "holder_province","holder_city", "holder_district","holder_street","holder_zipno","holder_company",
        "holder_company_addr","holder_contact_name", "holder_contact_mobile","holder_contact_email","holder_org_code","holder_create_time","holder_update_time",
        "product_id","product_name","product_short_name","manage_org_name","product_type_code","type_name","pdt_original_price","pdt_current_price","minimum_premium",
        "sku_code","sku_str","sku_id", "sku_coverage","sku_append", "sku_ratio", "sku_price","sku_price_id", "sku_charge_type","invoice_type")

    val orderPolicyProductHolderInsurant = orderPolicyProductHolder.join(odrPolicyInsurantBznprd, orderPolicyProductHolder("master_policy_id") === odrPolicyInsurantBznprd("policy_id"), "leftouter")
      .selectExpr("master_order_id", "order_code", "user_id", "pay_amount",
        "contact_name","contact_mobile","contact_email","original_price","price","sales_name","order_create_time","order_update_time","master_policy_id", "applicant_code",
        "policy_code","insure_company_name","sku_id","policy_type","insure_code", "premium", "status","start_date", "end_date", "effect_date","renewal_policy_code",
        "order_date","channelId","channel_name", "policy_create_time", "policy_update_time",
        "holder_id","holder_type","holder_name", "holder_gender", "holder_cert_type","holder_cert_no","holder_birthday","holder_profession","holder_industry",
        "work_type","holder_email","holder_mobile","holder_nation", "holder_province","holder_city", "holder_district","holder_street","holder_zipno","holder_company",
        "holder_company_addr","holder_contact_name", "holder_contact_mobile","holder_contact_email","holder_org_code","holder_create_time","holder_update_time",
        "insurant_id","policy_id", "holder_relation", "insurant_cert_type","insurant_cert_no","insurant_province","insurant_city","insurant_company_name",
        "insurant_company_address","insurant_contact_name","insurant_contact_mobile","insurant_contact_email","insurant_create_time","insurant_update_time",
        "product_id","product_name","product_short_name","manage_org_name","product_type_code","type_name","pdt_original_price","pdt_current_price","minimum_premium",
        "sku_code","sku_str","sku_id", "sku_coverage","sku_append", "sku_ratio", "sku_price","sku_price_id", "sku_charge_type","invoice_type")

    val orderPolicyProductHolderInsurantItemOrder = orderPolicyProductHolderInsurant.join(odrOrderItemInfoBznprd, orderPolicyProductHolderInsurant("master_order_id") === odrOrderItemInfoBznprd("order_id"), "leftouter")
      .selectExpr("master_order_id", "order_code", "user_id", "pay_amount",
        "contact_name","contact_mobile","contact_email","original_price","price","sales_name","order_create_time","order_update_time","master_policy_id", "applicant_code",
        "policy_code","insure_company_name","sku_id","policy_type","insure_code", "premium", "status","start_date", "end_date", "effect_date",
        "order_date","channelId","channel_name", "policy_create_time", "policy_update_time",
        "holder_id","holder_type","holder_name", "holder_gender", "holder_cert_type","holder_cert_no","holder_birthday","holder_profession","holder_industry",
        "work_type","holder_email","holder_mobile","holder_nation", "holder_province","holder_city", "holder_district","holder_street","holder_zipno","holder_company",
        "holder_company_addr","holder_contact_name", "holder_contact_mobile","holder_contact_email","holder_org_code","holder_create_time","holder_update_time",
        "insurant_id","policy_id", "holder_relation", "insurant_cert_type","insurant_cert_no","insurant_province","insurant_city","insurant_company_name",
        "insurant_company_address","insurant_contact_name","insurant_contact_mobile","insurant_contact_email","insurant_create_time","insurant_update_time",
        "product_id","product_name","product_short_name","manage_org_name","product_type_code","type_name","pdt_original_price","pdt_current_price","minimum_premium",
        "sku_code","sku_str","sku_id", "sku_coverage","sku_append", "sku_ratio", "sku_price","sku_price_id", "sku_charge_type","quantity as number_of_pople","renewal_policy_code",
        "'1.0' as system_source","invoice_type")

    val resTemp = orderPolicyProductHolderInsurantItemOrder.join(entRes,orderPolicyProductHolderInsurantItemOrder("policy_code")===entRes("policy_code_ent_map"),"leftouter")
      .selectExpr("master_order_id as order_id", "order_code", "user_id", "pay_amount",
        "contact_name","contact_mobile","contact_email","original_price","price","sales_name","order_create_time","order_update_time","master_policy_id", "applicant_code",
        "policy_code","insure_company_name","sku_id","policy_type","insure_code", "premium", "status as policy_status","start_date", "end_date", "effect_date",
        "order_date","channelId","channel_name", "policy_create_time", "policy_update_time",
        "holder_id","holder_type","holder_name", "holder_gender", "holder_cert_type","holder_cert_no","holder_birthday","holder_profession","holder_industry",
        "work_type","holder_email","holder_mobile","holder_nation", "holder_province","holder_city", "holder_district","holder_street","holder_zipno","holder_company",
        "holder_company_addr","holder_contact_name", "holder_contact_mobile","holder_contact_email","holder_org_code","holder_create_time","holder_update_time",
        "insurant_id","policy_id", "holder_relation", "insurant_cert_type","insurant_cert_no","insurant_province","insurant_city","insurant_company_name",
        "insurant_company_address","insurant_contact_name","insurant_contact_mobile","insurant_contact_email","insurant_create_time","insurant_update_time",
        "product_id","product_name","product_short_name","manage_org_name","product_type_code","type_name","pdt_original_price","pdt_current_price","minimum_premium",
        "sku_code","sku_str","sku_id", "sku_price","sku_coverage","sku_append","sku_charge_type","sku_ratio", "sku_price_id","ent_id","ent_code","ent_name",
        "license_code","org_code","tax_code","join_social","office_address",
        "case when office_province is null then holder_province else office_province end as office_province",
        "case when office_city is null then holder_city else office_city end as office_city","ent_contact_name","ent_contact_duty","ent_contact_email",
        "ent_contact_mobile","ent_contact_telephone","ent_type","ent_biz_address","ent_status","tax_address","ent_telephone","ent_bank_name","ent_bank_account",
        "number_of_pople","renewal_policy_code", "'1.0' as system_source","invoice_type")
      .cache()


    val orderPolicyProductHolderInsurantItemOrderone = resTemp
      .where("order_id not in ('934cec7f92f54be7812cfcfa23a093cb') and (user_id not in ('10100080492') or user_id is null) and insure_code not in ('15000001')")

    val orderPolicyProductHolderInsurantItemOrderTwo = resTemp
      .where("insure_code in ('15000001') and (user_id not in ('10100080492') or user_id is null)")
    val res = orderPolicyProductHolderInsurantItemOrderone.unionAll(orderPolicyProductHolderInsurantItemOrderTwo)
    //    res.write.mode(SaveMode.Overwrite).saveAsTable("odsdb.ods_policy_detail")

    /**
      * 读取产品明细表,将蓝领外包以外的数据进行处理，用总保费替换初投保费
      */
//    val odsProductDetail = sqlContext.sql("select product_code as product_code_slave,product_type_2 from odsdb_prd.dim_product")
//      .where("product_type_2 <> '蓝领外包'")

    val resEnd = res
//      .join(odsProductDetail, res("insure_code") === odsProductDetail("product_code_slave"), "leftouter")
      .selectExpr("getUUID() as id","order_id", "order_code", "user_id", "contact_name","contact_mobile","contact_email","original_price","price",
        "pay_amount","sales_name","order_create_time","order_update_time",
        "master_policy_id as policy_id", "applicant_code","policy_code","insure_company_name","sku_id","policy_type","insure_code", "premium", "start_date",
        "end_date", "effect_date","order_date","policy_status","channelId","channel_name", "policy_create_time", "policy_update_time",
        "holder_id","holder_type","holder_name", "holder_gender", "holder_cert_type","holder_cert_no","holder_birthday","holder_profession","holder_industry",
        "work_type","holder_email","holder_mobile","holder_nation", "holder_province","holder_city", "holder_district","holder_street","holder_zipno","holder_company",
        "holder_company_addr","holder_contact_name", "holder_contact_mobile","holder_contact_email","holder_org_code","holder_create_time","holder_update_time",
        "insurant_id","holder_relation", "insurant_cert_type","insurant_cert_no","insurant_province","insurant_city","insurant_company_name",
        "insurant_company_address","insurant_contact_name","insurant_contact_mobile","insurant_contact_email","insurant_create_time","insurant_update_time",
        "product_id","product_name","product_short_name","manage_org_name","product_type_code","type_name","pdt_original_price","pdt_current_price",
        "case when minimum_premium = '' then null else minimum_premium end as minimum_premium",
        "sku_code","sku_str", "sku_price","sku_coverage","sku_append","sku_charge_type","sku_ratio", "sku_price_id","ent_id","ent_code","ent_name",
        "license_code","org_code","tax_code","case when join_social = '' then null else join_social end as join_social","office_address",
        "office_province","office_city","ent_contact_name","ent_contact_duty","ent_contact_email","ent_contact_mobile","ent_contact_telephone",
        "ent_type","ent_biz_address","case when ent_status = '' then null else ent_status end as ent_status","tax_address","ent_telephone",
        "ent_bank_name","ent_bank_account","number_of_pople","renewal_policy_code", "'1.0' as system_source","invoice_type")

    resEnd
  }

  //删除hdfs的文件，后输出
  def delete(path: String, tep_end: RDD[String]): Unit = {
    val properties = getProPerties()
    val hdfsUrl = properties.getProperty("hdfs_url")
    val output = new org.apache.hadoop.fs.Path(hdfsUrl + path)
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(hdfsUrl), new org.apache.hadoop.conf.Configuration())
    // 删除输出目录
    if (hdfs.exists(output)) {
      hdfs.delete(output, true)
      println("delete!--" + hdfsUrl + path)
      tep_end.repartition(1).saveAsTextFile(path)
    } else tep_end.repartition(1).saveAsTextFile(path)
  }

  /**
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
      .option("url", properties.getProperty("mysql.url.105"))
      .option("driver", properties.getProperty("mysql.driver"))
      .option("user", properties.getProperty("mysql.username.105"))
      .option("password", properties.getProperty("mysql.password.105"))
      .option("numPartitions", "10")
      .option("partitionColumn", "id")
      .option("lowerBound", "0")
      .option("upperBound", "200")
      .option("dbtable", tableName)
      .load()
  }
  /**
    * 获取 Mysql 表的数据非id 的
    *
    * @param sqlContext
    * @param tableName 读取Mysql表的名字
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlTableNoId(sqlContext: SQLContext, tableName: String): DataFrame = {
    val properties: Properties = getProPerties()
    sqlContext
      .read
      .format("jdbc")
      .option("url", properties.getProperty("mysql.url.105"))
      .option("driver", properties.getProperty("mysql.driver"))
      .option("user", properties.getProperty("mysql.username.105"))
      .option("password", properties.getProperty("mysql.password.105"))
      .option("dbtable", tableName)
      .load()
  }

  /**
    * 获取配置文件
    *
    * @return
    */
  def getProPerties() = {
    val lines_source = Source.fromURL(getClass.getResource("/config-scala.properties")).getLines.toSeq
    var properties: Properties = new Properties()
    for (elem <- lines_source) {
      val split = elem.split("==")
      val key = split(0)
      val value = split(1)
      properties.setProperty(key, value)
    }
    properties
  }
}

