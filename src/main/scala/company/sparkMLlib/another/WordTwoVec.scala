package company.sparkMLlib.another

import org.ansj.recognition.impl.StopRecognition
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

case class Document(docContent: String)

object WordTwoVec {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)


  case class Movies(id: String, stype: String, mtype: String,
                    ltype: String, li_level: String, ai_level: String, mi_level: String)

  def main(args: Array[String]): Unit = {
    //分词准备
    val stop = new StopRecognition()
    stop.insertStopNatures("w") //过滤掉标点
    stop.insertStopNatures("m") //过滤掉m词性
    stop.insertStopNatures("null") //锅炉掉ull词性
    stop.insertStopNatures(":")
    stop.insertStopNatures(",")
    stop.insertStopNatures("(")
    stop.insertStopNatures(")")


    val conf = new SparkConf().setMaster("local[2]").setAppName("Vector")
    val sc = new SparkContext(conf)
    val sql = new SQLContext(sc)


    //众安标准工种分类,使用rdd进行读取然后创建HiveContext
    val splits = sc.textFile("C:\\Users\\a2589\\Desktop\\tmp\\company\\d_worktype_za")
      .map(x => {
        val result = x.split(",")
        result(0) + "\t" + result(1) + "\t" + result(2) + "\t" + result(3) + "\t" + result(4) + "\t" + result(5) + "\t" + result(6)
      })
    //生成训练数据集
    import sql.implicits._
    val trains = splits.map(_.split("\t")).map(x => Movies(x(0), x(1), x(2), x(3), x(4), x(5), x(6))).toDF()
    //        trains.show() //show方法默认可以查看钱20行数据，如果想多看点可以给定数值

    //定义临时表才可以在后续使用sql语句查看
    trains.registerTempTable("train")
    //末尾加rdd可以将DateFrame转换为RDD
    val doc = sql.sql("select stype from train").rdd

    /**
      * 测试对下面的该行句子进行分词，将分次输出的结果使用"|"进行分割
      **/
    //    val testsentence = DicAnalysis
    //      .parse("企事业单位部门经理主管(不亲自作业)")
    //      .recognition(stop).toStringWithOutNature("|")
    //    println(testsentence)
    /**
      * 对该字段的文档进行分词处理
      **/
    //    val splited = doc.map(x => {
    //      val str = x.toString()
    //      DicAnalysis.parse(str).recognition(stop).toStringWithOutNature("\t")
    //    }).repartition(1).saveAsTextFile("C:\\Users\\a2589\\Desktop\\tmp\\analys_data")

//    val input = sc.textFile("C:\\Users\\a2589\\Desktop\\tmp\\analys_data\\part-00000").map(line => line.split("\t").toSeq)
//    val word2vec = new Word2Vec()
//    word2vec.setMinCount(1)
//    word2vec.setNumPartitions(1)
//    word2vec.setNumIterations(1)
//    val model = word2vec.fit(input)
//    val synonyms = model.findSynonyms("工人", 10)
//    for ((synonym, cosineSimilarity) <- synonyms) {
//      println(s"$synonym $cosineSimilarity")
//    }
    //
    //    //对输入的数据进行分词
    //    val data_testsentence = DicAnalysis
    //      .parse("办公室公务人员")
    //      .recognition(stop).toStringWithOutNature("\t")
    //
    //    val result = data_testsentence.split("\t")
    //    val synonyms = model.findSynonyms("焊工", 10)
    //    for ((synonym, cosineSimilarity) <- synonyms) {
    //      println(s"$synonym $cosineSimilarity")
    //    }


    //    model.save(sc, "inpath")

    sc.stop()
  }
}
