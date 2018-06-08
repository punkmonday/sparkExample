package org.apache.spark.examples.streaming

import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object Application extends App {
  //初始化sparkSession
  val spark = SparkSession.builder.master("local[*]").appName("StructuredStreamingTest").getOrCreate()
  //隐式导入，必须，否则会报错
  spark.readStream
    .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test")
    .option("startingOffsets", "earliest")
    .load()
    .writeStream
    .trigger(Trigger.ProcessingTime(10000))
    .outputMode("update")
    .format("org.apache.spark.examples.streaming.provider.DemoSinkProvider")
    .option("checkpointLocation", "/tmp/temporary-" + UUID.randomUUID.toString)
    .start().awaitTermination()
}
