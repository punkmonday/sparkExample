package org.apache.spark.examples.streaming.test

import org.apache.spark.sql.SparkSession

/**
  * 此程序演示spark structure streaming获取kafka stream并输出到控制台
  *
  */
object StructuredStreamingBatchQueryTest {

  def main(args: Array[String]): Unit = {
    //初始化sparkSession
    val spark = SparkSession.builder.master("local[*]").appName("StructuredStreamingTest").getOrCreate()
    //隐式导入，必须，否则会报错
    spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .write
      .format("console")
      .save
  }
}
