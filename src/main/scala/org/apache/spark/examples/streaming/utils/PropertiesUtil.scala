package org.apache.spark.examples.streaming.utils

import java.util.Properties

object PropertiesUtil {

  val configProp = "config.properties" //固定读取config.properties文件

  var properties: Properties = _

  {
    properties = new Properties()
    properties.load(this.getClass.getClassLoader.getResourceAsStream(configProp)) //文件要放到resource文件夹下
  }

  def getProperty(prop: String): String = {
    properties.getProperty(prop)
  }

}
