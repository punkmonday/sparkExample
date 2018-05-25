package org.apache.spark.examples.streaming.test

import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

import scala.concurrent.duration._

/**
  * 此程序演示spark structure streaming获取kafka stream并输出到控制台
  *
  */
object CustomStreamingSinkTest {

  def main(args: Array[String]): Unit = {
    //初始化sparkSession
    val spark = SparkSession.builder.master("local[*]").appName("StructuredStreamingTest").getOrCreate()
    //隐式导入，必须，否则会报错
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .option("startingOffsets", "earliest")
      .load()
      .writeStream
      .trigger(Trigger.ProcessingTime(10.seconds))
      .outputMode("update")
      .format("demo")
      .option("checkpointLocation", "/tmp/temporary-" + UUID.randomUUID.toString)
      .start().awaitTermination()
  }
}
