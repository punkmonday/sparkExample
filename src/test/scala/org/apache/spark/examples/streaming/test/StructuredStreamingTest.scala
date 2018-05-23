package org.apache.spark.examples.streaming.test

import java.util.UUID

import org.apache.spark.sql.SparkSession

/**
  * 此程序演示spark structure streaming获取kafka stream并输出到控制台
  *
  */
object StructuredStreamingTest {

  def main(args: Array[String]): Unit = {
    //初始化sparkSession
    val spark = SparkSession.builder.master("local[*]").appName("StructuredStreamingTest").getOrCreate()
    //隐式导入，必须，否则会报错
    import spark.implicits._
    //session监听socket
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "cdh-master:9092")
      .option("subscribe", "MysqlTestTopic")
      .option("startingOffsets", "earliest")
      .option("auto.offset.reset", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .writeStream
      .outputMode("append")
      .format("console")
      .option("checkpointLocation", "/tmp/temporary-" + UUID.randomUUID.toString)
      .start().awaitTermination()
  }
}
