package com.bzn.cLabel

object test {
  def main(args: Array[String]): Unit = {
    val str = "2017-10-09 08:07:34"
    val test = ""+"\u0001"+""+"132"+"\u0001"+"132"+"\u0001"
//    91110108348452091Y
//    231085199502280515
    val shen = "231085199502280515"
    println(shen.substring(shen.length-2,shen.length-1))
    println(cardCodeVerifySimple(shen))
//    println(test.trim.split("\u0001").distinct(0))
//    println(test.trim.split("\u0001").distinct.mkString("\u0001").split("\u0001").length)
//    println(str.length)
    if(str == ""){
      println("this is null")
    }
  }

  private def cardCodeVerifySimple(cardcode: String): Boolean = {
    val isIDCard1 = "^[1-9]\\d{7}((0\\d)|(1[0-2]))(([0|1|2]\\d)|3[0-1])\\d{3}$"
    //第二代身份证正则表达式(18位)
    val isIDCard2 = "^[1-9]\\d{5}[1-9]\\d{3}((0\\d)|(1[0-2]))(([0|1|2]\\d)|3[0-1])((\\d{4})|\\d{3}[A-Z])$"
    //验证身份证
    if (cardcode.matches(isIDCard1) || cardcode.matches(isIDCard2))
      return true
    false
  }
}
