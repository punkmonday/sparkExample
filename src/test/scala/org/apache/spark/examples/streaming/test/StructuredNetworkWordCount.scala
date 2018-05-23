package org.apache.spark.examples.streaming.test

import org.apache.spark.sql.SparkSession

/**
  * 此程序演示spark structure streaming监听socket接口，做词频统计
  * 使用方法： 目前参数已经写到代码里 host: localhost port: 9999
  * 在本机运行netcat： nc -lk 9999,接收输入，随便输入单词回车，程序就会实时获取流并统计
  */
object StructuredNetworkWordCount {

  def main(args: Array[String]): Unit = {
    //初始化sparkSession
    val spark = SparkSession.builder.master("local[*]").appName("StructuredNetworkWordCount").getOrCreate()
    //隐式导入，必须，否则会报错
    import spark.implicits._
    //session监听socket
    val lines = spark.readStream.format("socket").option("host", "localhost").option("port", "9999").load()
    //根据空格分割单词
    val words = lines.as[String].flatMap(_.split(" "))
    //根据value统计词频
    val wordCount = words.groupBy("value").count()
    //写流到控制台
    val query = wordCount.writeStream.outputMode("complete").format("console").start()

    query.awaitTermination()
  }

}
