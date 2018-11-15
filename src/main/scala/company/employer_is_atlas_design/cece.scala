package company.employer_is_atlas_design

import java.io.StringReader

import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by MK on 2018/9/7.
  * 雇主的图谱设计
  */
object cece {
  def main(args: Array[String]): Unit = {
    val conf_s = new SparkConf().setAppName("cece").setMaster("local[2]")
    val sc = new SparkContext(conf_s)

    val sQLContext: SQLContext = new SQLContext(sc)

    val big_tepOne = sc.textFile("C:\\Users\\a2589\\Desktop\\雇主图谱设计\\guzhucsv.csv").map(x => {
      val reader = new CSVReader(new StringReader(x.replaceAll("\"", "")))
      reader.readNext()
    })
    val schema_tepOne = StructType(big_tepOne.first.map(fieldName => StructField(fieldName, StringType, nullable = true)))
    //字段对应的值
    val value_tepTwo = big_tepOne.map(r => Row(r: _*))


    val big = sQLContext.createDataFrame(value_tepTwo, schema_tepOne)
      .filter("公司='众安'").drop("工种").map(x => x.toSeq.toList.map(_.toString).mkString(",")).collect()


    sc.textFile("C:\\Users\\a2589\\Desktop\\雇主图谱设计\\guzhu.txt").distinct.map(_.replace(",", "")).flatMap(a => {
      big.map(x=>s"$x,$a")
    }).repartition(1).saveAsTextFile("C:\\Users\\a2589\\Desktop\\雇主图谱设计\\end.txt")


  }
}
