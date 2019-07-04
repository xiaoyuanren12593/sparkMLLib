import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object test {
  def main(args: Array[String]): Unit = {
    println(textCosine("青岛锦峰源机械设备安装有限公司", "青岛海航机械设备有限公司"))
    println(textCosine("青岛锦峰源机械设备安装有限公司", "青岛锦峰源机械设备安装有限公司"))
    println(" ".length)
    ListMap(List(("zhejiang", 0.95), ("123", 0.94), ("zee", 0.98)).toMap.toSeq.sortWith(_._2<_._2):_*).foreach(println)
    List(("zhejiang", 0.95), ("123", 0.94), ("zee", 0.98)).toMap.toSeq.foreach(println)
    val grades = Map("Kim" -> 90,
         "Al" -> 85,
       "Melissa" -> 90,
       "Emily" -> 91,
       "Hannah" -> 92
    )
    println(ListMap(grades.toSeq.sortBy(_._2): _*).last)

  }
  // 返回三个数中的最大值
  def getMax(a:Double, b:Double, c:Double): Double = {
    if (a > b) {
      if (a > c) {
        return a
      } else if (a == c) {
        return -1
      } else {
        return c
      }
    } else if (a == b) {
      if (c > a) {
        return c
      } else {
        return -1
      }
    } else {
      if (b > c) {
        return b
      } else if (b == c) {
        return -1
      } else {
        return c
      }
    }
  }

  /**
    * 求向量的模
    * @param vec
    * @return
    */
  def module(vec: Vector[Double]) = {
    math.sqrt(vec.map(math.pow(_, 2)).sum)
  }

  /**
    * 求两个向量的内积
    * @param v1
    * @param v2
    * @return
    */
  def innerProduct(v1: Vector[Double], v2: Vector[Double]) =
  {
    val listBuffer = ListBuffer[Double]()
    for (i <- 0 until v1.length; j <- 0 to v2.length; if i == j) {
      if (i == j)
        listBuffer.append(v1(i) * v2(j))
    }
    listBuffer.sum
  }

  /**
    * 求两个向量的余弦
    * @param v1
    * @param v2
    * @return
    */

  def cosvec(v1: Vector[Double], v2: Vector[Double]) = {
    val cos = innerProduct(v1, v2) / (module(v1) * module(v2))
    if (cos <= 1) cos else 1.0
  }

  /**
    * 余弦相似度
    * @param str1
    * @param str2
    * @return
    */
  def textCosine(str1: String, str2: String) = {
    val set = mutable.Set[Char]()
    // 不进行分词
    str1.foreach(set += _)
    str2.foreach(set += _)
    val ints1: Vector[Double] = set.toList.sorted.map(ch => {
      str1.count(s => s == ch).toDouble }).toVector
    val ints2: Vector[Double] = set.toList.sorted.map(ch => {
      str2.count(s => s == ch).toDouble
    }).toVector

    println(set.toList.sorted.map(ch => {
      str2.count(s => s == ch).toDouble
    }))
    cosvec(ints1, ints2)
  }
}
