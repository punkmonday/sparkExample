package org.apache.spark.examples.streaming.utils

import java.io.{FileInputStream, InputStream}
import java.util.Properties

object PropertiesUtil {

  var properties = new Properties()
  properties.load(this.getClass.getClassLoader.getResourceAsStream("config.properties")) //文件要放到resource文件夹下

  def load(inputStream: InputStream): Unit = {
    properties.load(inputStream)
  }

  def load(path: String): Unit = {
    properties.load(new FileInputStream(path))
  }

  def getProperty(prop: String): String = {
    properties.getProperty(prop)
  }

  def main(args: Array[String]): Unit = {
    println(PropertiesUtil.getProperty("config.app.name"))
  }

}
