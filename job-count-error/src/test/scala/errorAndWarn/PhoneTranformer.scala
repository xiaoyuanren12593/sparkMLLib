package errorAndWarn

import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.HttpURLConnection
import java.net.URL
import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


object PhoneTranformer {
  def main(args: Array[String]): Unit = {
    val confs = new SparkConf()
      .setAppName("wuYu")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
      .set("spark.sql.broadcastTimeout", "36000")
      .setMaster("local[4]")


    val prop: Properties = new Properties

    val sc = new SparkContext(confs)

    val sqlContext: HiveContext = new HiveContext(sc)

    import sqlContext.implicits._

    val mobile_tmp: DataFrame =  sqlContext.sql("select phone_number from bzn_open.mobile_tmp")

    val res: RDD[(String, String, String, String, String)] = mobile_tmp.map(x => x.toString().substring(1,x.toString().length-1 )).map(x => {
      val res = getPhone(x.toString())
      val split = res.split(" ")
      val printPhone = split(0)
      val detailProvince = split(1)
      val detailAreaCity = split(2)
      val detailAreaDistrict = split(3)
      val detailType = split(4)
      (printPhone,detailProvince,detailAreaCity,detailAreaDistrict,detailType)
    })
    val result = res.toDF("printPhone","detailProvince","detailAreaCity","detailAreaDistrict","detailType")
    val table_name = "mobile_parse"
    result.show(10000)
//    result.insertInto(s"bzn_open.$table_name", overwrite = true)
  }

  //拼接解析的信息
  def getPhone(phoneParam: String): String ={
    //{"response":{"04555351552":{"detail":{"area":[{"city":"绥化","district":["兰西"]}],"province":"黑龙江","type":"domestic"},"location":"黑龙江绥化"}},"responseHeader":{"status":200,"time":1551943404775,"version":"1.1.0"}}
    val phone: String = PhoneTranformer.httpRequest(phoneParam).mkString("")
    val response = JSON.parse(phone)//response
    val phoneFirst = JSON.parseObject(response.toString).get("response")
    //手机号
    val printPhone = phoneParam
    val phoneSecond = JSON.parseObject(phoneFirst.toString).get(phoneParam)
    val printLocation = JSON.parseObject(phoneSecond.toString).get("location")
    val detail = JSON.parseObject(phoneSecond.toString).get("detail")
    val detailArea = JSON.parseObject(detail.toString).get("area")
    val detailArea_ = JSON.parseObject(detailArea.toString.substring(1,detailArea.toString.length-1))
    //城市
    val detailAreaCity = detailArea_.get("city")
    //县级
    val detailAreaDistrict = detailArea_.get("district")
    //省份
    val detailProvince = JSON.parseObject(detail.toString).get("province")
    //类型
    val detailType = JSON.parseObject(detail.toString).get("type")
    val res = printPhone + " " +detailProvince + " " +detailAreaCity + " " + detailAreaDistrict + " "+detailType
//    println(res)
    res
  }

  //解析手机号
  def httpRequest(tel: String): String = {
    //组装查询地址(requestUrl 请求地址)
    //        String requestUrl = "http://api.k780.com:88/?app=phone.get&phone="+tel+"&appkey=10003&sign=b59bc3ef6191eb9f747dd4e83c99f2a4&format=xml";
    val requestUrl = "http://mobsec-dianhua.baidu.com/dianhua_api/open/location?tel=" + tel
    val buffer = new StringBuffer
    try {
      val url = new URL(requestUrl)
      val httpUrlConn = url.openConnection.asInstanceOf[HttpURLConnection]
      httpUrlConn.setDoOutput(false)
      httpUrlConn.setDoInput(true)
      httpUrlConn.setUseCaches(false)
      httpUrlConn.setRequestMethod("GET")
      httpUrlConn.connect()
      //将返回的输入流转换成字符串
      var inputStream = httpUrlConn.getInputStream
      val inputStreamReader = new InputStreamReader(inputStream, "UTF-8")
      val bufferedReader = new BufferedReader(inputStreamReader)
      var str :String =  bufferedReader.readLine
      buffer.append(str)
//      while ((str = bufferedReader.readLine) != null)
      bufferedReader.close()
      inputStreamReader.close()
      //释放资源
      inputStream.close()
      inputStream = null
      httpUrlConn.disconnect()
    } catch {
      case e: Exception =>
        return "发起http请求后，获取返回结果失败！"
    }
    buffer.toString
  }
}

