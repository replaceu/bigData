package foreo.utils

import jline.internal.InputStreamReader

import java.nio.charset.StandardCharsets
import java.util.Properties


/**
 * 读取配置文件的工具类
 */
object ForeoPropUtils {
  def load(properties:String): Properties ={
    val prop: Properties = new Properties()
    //todo:加载指定的配置文件
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(properties),StandardCharsets.UTF_8))
    prop
  }
}
