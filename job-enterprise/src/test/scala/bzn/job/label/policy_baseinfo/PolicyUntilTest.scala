package bzn.job.label.policy_baseinfo

import bzn.job.common.Until
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * Created by a2589 on 2018/4/3.
  */
trait PolicyUntilTest extends Until {

  //产品编号
  def policy_insure_code(ods_policy_detail: DataFrame): RDD[(String, String, String)] = {
    // 保单号|(险种,产品代码)
    val end: RDD[(String, String, String)] = ods_policy_detail
      .filter("length(policy_code) > 4 and length(insure_code) > 4")
      .select("policy_code", "insure_code")
      .map(x => (x.getString(0), x.getString(1), "policy_insure_code"))

    end
  }

  //保单生效时间
  def policy_start_date(ods_policy_detail: DataFrame): RDD[(String, String, String)] = {
    //start_date:保险起期
    val end = ods_policy_detail
      .filter("length(policy_code) > 4 and length(start_date)>0")
      .select("policy_code", "start_date")
      .map(x => {
        val data = x.getString(1).split(" ")(0)
        (x.getString(0), data, "policy_start_date")
      })

    end
  }

  //保单截至时间
  def policy_end_date(ods_policy_detail: DataFrame): RDD[(String, String, String)] = {
    val end = ods_policy_detail
      .filter("length(policy_code) > 4 and length(end_date)>0")
      .select("policy_code", "end_date")
      .map(x => {
        val data = x.getString(1).split(" ")(0)
        (x.getString(0), data, "policy_end_date")
      })

    end
  }

  //保费
  def policy_premium(ods_policy_detail: DataFrame): RDD[(String, String, String)] = {
    val end: RDD[(String, String, String)] = ods_policy_detail.filter("length(policy_code) > 4").select("policy_code", "premium").map(x => {
      if(x.get(1) == null){
        (x.getString(0),"", "policy_premium")
      }else{
        (x.getString(0), x.get(1).toString, "policy_premium")
      }
    })
    end
  }

  //保单状态
  def policy_status(ods_policy_detail: DataFrame): RDD[(String, String, String)] = {
    val end = ods_policy_detail
      .filter("length(policy_code) > 4")
      .select("policy_code", "policy_status")
      .map(x => (x.getString(0), x.get(1).toString, "policy_status"))

    end
  }

  //保单更新时间
  def policy_update_time(ods_policy_detail: DataFrame): RDD[(String, String, String)] = {
    val end = ods_policy_detail
      .filter("length(policy_code) > 4 and length(policy_update_time)>0")
      .select("policy_code", "policy_update_time")
      .map(x => {
        val data = x.getString(1).split(" ")(0)
        (x.getString(0), data, "policy_update_time")
      })

    end
  }

  //保额
  def policy_term_three(ods_policy_detail: DataFrame, pdt_product_sku: DataFrame): RDD[(String, String, String)] = {
    //pdt_product_sku	官网产品sku表
    val tep_one = ods_policy_detail
      .filter("length(policy_code) > 0")
      .select("policy_code", "sku_code")
      .distinct()
    val tep_two = pdt_product_sku
      .select("sku_code", "term_three")
      .filter("term_three is not null")
    val tep_three = tep_one.join(tep_two, "sku_code")
    //    | sku_code|       policy_code|term_three|
    //    |710000903|900000047702719243|     10212|
    val end: RDD[(String, String, String)] = tep_three
      .map(x => (x.getString(1), x.get(2).toString, "policy_term_three"))
      .filter(x => if (x._2.size > 3) true else false)
      .map(x => (x._1, x._2.substring(0, 2), x._3))

    end
  }

}
