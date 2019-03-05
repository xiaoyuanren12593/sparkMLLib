package errorAndWarn

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CountErrorAndWarnTest {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf_spark = new SparkConf()
      .setAppName("xingyuan")
    conf_spark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf_spark.registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
    conf_spark.set("spark.sql.broadcastTimeout", "36000")
      .setMaster("local[2]")

    val sc = new SparkContext(conf_spark)

    val read = sc.textFile("C:\\Users\\xingyuan\\Desktop\\otherlogs\\*.log")

    val res = read.map(x => {
      //去掉数据中自带的换行符
        (x.replace("\\r\\n",""))
      }).filter(x => {
      //过滤脏数据
      val length = x.toString.length
      length >= 10
    }).filter(x => {
      //过滤掉没有日期的数据
      val res = x.toString.substring(0,10)
      is_not_date(res)
    }).map(x => {
      //将截取日期
      val date = x.substring(0,23)
      //截取错误类型 error  warn
      val typeDate = x.substring(24,31)
      //[ERROR]之后的数据  临时存储
      val lastTmp = x.substring(32,x.length)
//        [XNIO-3 task-127] c.b.d.i.service.claimmodel.ClaimDecisionsService2.decisions(ClaimDecisionsService2.java:130) - 未查询到企业风险/价值数据为空 或 未查询到个人风险数据
      //读取错误出现的类的前后索引
      val threePara_index_start = lastTmp.indexOf("(")
      val threePara_index_end = lastTmp.indexOf(")")
      //截取出现错误的类
      val threePara = lastTmp.substring(threePara_index_start+1,threePara_index_end).trim
//      println(threePara)
//      println(lastTmp.substring(lastTmp.indexOf("]")+1,lastTmp.length-1))
      // - 后存储的是错误产生的原因
      val index: Int = lastTmp.substring(lastTmp.indexOf("]")+1,lastTmp.length).indexOf("-")
      //过滤掉不存在原因的数据
      var fouth = ""
      if(index == -1){
      }else{
        val temp = lastTmp.substring(lastTmp.indexOf("]")+1,lastTmp.length)
        fouth = temp.substring(temp.indexOf("-")+1,temp.length)
      }
      // 错误类型   错误出现的类  错误原因   时间
      (typeDate,threePara,fouth.trim,date)
    })
      //过滤原因为“” 的数据
      .filter(x => x._3 != "")
      //过滤为ERROR的数据
      .filter(x => x._1 == "[ERROR]")
      .map(x => {
        //将错误类型和错误原因以及错误类作为key   时间和次数作为值
        ((x._1+"["+x._2+"]"+"["+x._3+"]"),("["+x._4+"]",1))
      })
      .reduceByKey((x1,x2) => {
        //对日期进行拼接
        val res1 = x1._1 +"|"+x2._1
        //将相同key值累加
        val res2 = x1._2+x2._2
        (res1,res2)
      })
      val dateSorted = res
      .map(x => {
        //取出时间为最近的日期
        val res = x._2._1.split("\\|").sorted.reverse(0)
        //相同错误总数   最近时间 错误类型  错误的类及位置 错误的原因
        (res,(x._2._2,x._1))
      })
      //倒叙
      .sortByKey(false).map(x => {
      (x._2._1+x._1+x._2._2)
    }).coalesce(1,true).saveAsTextFile("D:\\test_7.log")

      val CountSorted =res
        .map(x => {
          //取出时间为最近的日期
          val res = x._2._1.split("\\|").sorted.reverse(0)
          //相同错误总数   最近时间 错误类型  错误的类及位置 错误的原因
          (x._2._2,(res,x._1))
        })
        //倒叙
        .sortByKey(false)
        .coalesce(1,true).saveAsTextFile("D:\\test_8.log")

     sc.stop()
  }

  //判断是否是时间
  def is_not_date(str: String): Boolean
  = {
    var convertSuccess: Boolean = true
    // 指定日期格式为四位年/两位月份/两位日期，注意yyyy/MM/dd区分大小写；
    var format = new SimpleDateFormat("yyyy-MM-dd")
    // 设置lenient为false. 否则SimpleDateFormat会比较宽松地验证日期，比如2007/02/29会被接受，并转换成2007/03/01
    try {
      format.setLenient(false);
      format.parse(str)
    } catch {
      case e => convertSuccess = false
    };
    convertSuccess
  }
}
