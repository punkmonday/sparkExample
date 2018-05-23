package org.apache.spark.examples.streaming.test

import java.io.FileInputStream
import java.util.Properties

object ConfigPropertiesTest {

  val configProp = "config.properties"

  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    val path = Thread.currentThread().getContextClassLoader.getResource(configProp).getPath //文件要放到resource文件夹下
    properties.load(new FileInputStream(path))

    println(properties.getProperty("config.name")) //读取属性
    println(properties.getProperty("config.cn.name")) //读取属性
    println(properties.getProperty("name", "没有值")) //如果属性不存在,则返回第二个参数

    properties.setProperty("config.name.name", "172.16.10.23") //添加或修改属性值
    println(properties.getProperty("config.name.name"))
  }

}
