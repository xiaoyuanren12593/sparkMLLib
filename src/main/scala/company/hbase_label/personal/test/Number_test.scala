package company.hbase_label.personal.test

object Number_test {
  def main(args: Array[String]): Unit = {
      val res = "201902-201903-201904-0"
    println(res.split("-").filter(x => {
      x.length == 6
    }).distinct.mkString("-"))
      println(res.substring(0,res.lastIndexOf("-")))
  }
}
