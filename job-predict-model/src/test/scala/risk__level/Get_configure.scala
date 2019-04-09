package risk__level

import java.io.FileInputStream
import java.util.Properties

/**
  * Created by MK on 2018/5/8.
  */
class Get_configure {
  def get(): Properties = {
    val pro: Properties = new Properties()
    var in: FileInputStream = null
    val path: String = Thread.currentThread.getContextClassLoader.getResource("application.properties").getPath
    in = new FileInputStream(path)
    pro.load(in)
    pro
  }
}
