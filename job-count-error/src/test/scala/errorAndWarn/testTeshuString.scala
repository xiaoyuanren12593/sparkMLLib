package errorAndWarn

object testTeshuString {
  def main(args: Array[String]): Unit = {
    val str:String = "测试特殊字符  到底能不能查出有几   个"

    if(str.contains("\\t")){
      println("有 \\ t")
    }else{
      println("no have \\ t")
    }
  }
}
