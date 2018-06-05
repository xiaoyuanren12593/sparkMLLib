package company.sparkMLlib

/**
  * Created by MK on 2018/5/7.
  */
object 最小二乘 {
  def before(x: Array[Double], y: Array[Double]): Unit = {

    //x求和
    val sumx = x.sum
    //y求和
    val sumy = y.sum

    //参数的个数
    val n = x.length

    //x的平均值
    val mx = sumx / n

    //y的平均值
    val my = sumy / n

    //x的平方和
    val x2 = x.map(Math.pow(_, 2)).sum

    //y的平方和
    val y2 = y.map(Math.pow(_, 2)).sum

    //x*y的和
    var sumxy = 0.0
    //indices返回所有的有效索引值
    for (i <- x.indices) {
      sumxy += x(i) * y(i)
    }

    val lxx = x2 - 1 / n * Math.pow(sumx, 2)
    val lyy = y2 - 1 / n * Math.pow(sumy, 2)

    val lxy = sumxy - 1 / n * sumx * sumy


    //a0
    val a0 = lxy / lxx

    val a1 = my - mx * a0

    println(a0, a1)
  }

  //最小二乘法
  def calCoefficientes(x: Array[Double], y: Array[Double]): (Double, Double) = {
    var xy: Double = 0
    var xT: Double = 0
    var yT: Double = 0
    var xS: Double = 0
    for (i <- x.indices) {
      xy += x(i) * y(i)
      xT += x(i)
      yT += y(i)
      xS += Math.pow(x(i), 2.0)
    }
    val num = x.length
    val a: Double = (num * xy - xT * yT) / (num * xS - Math.pow(xT, 2.0))
    val b: Double = yT / num - a * xT / num
    (a, b)
  }

  //预测
  def predicts(a: Double, b: Double, value: Double): Double = {
    a * value + b
  }

  def main(args: Array[String]): Unit = {

    //        val x: Array[Double] = Array(0.1, 0.11, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.2, 0.21, 0.23)
    //        val y: Array[Double] = Array(42, 43, 45, 45, 45, 47.5, 49, 53, 50, 55, 55, 60)
    //        val x: Array[Double] = Array(1801, 1802, 1803, 1804, 1805,1810)
    //        val y: Array[Double] = Array(321, 333, 452, 532, 641,743)
    //    val x: Array[Double] = Array(1801,1802)
    //    val y: Array[Double] = Array(321,456)
    //
    //    val tu_end = calCoefficientes(x, y)
    //        val a = tu_end._1
    //        val b = tu_end._2
    //        //预测
    ////        val value = predicts(a, b, 6)
    //        println(a)
    //        println(b)
    //        println(value)
    //    val day=new Date()
    //
    //    val df = new SimpleDateFormat("yyyyMM")
    //
    //    System.out.println(df.format(day))

  }
}
