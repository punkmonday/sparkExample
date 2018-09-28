package org.apache.spark.examples.streaming.test

import java.util.UUID

import org.apache.spark.examples.streaming.DataBaseConfig
import org.apache.spark.examples.streaming.utils.PropertiesUtil
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.OutputMode

/**
  * 程序演示从kafka获取,数据根据mysql数据库的配置,把JSON转为DataFrame,并进行正则和范围替换,清洗的数据保存到kudu数据库
  * 需要添加外部配置文件config.properties,参数参照src/main/resources/config.properties,需要3个表在sql ddl.sql
  */
object StructuredStreamingFilterTest extends App {
  if (args.length < 1) {
    System.err.println("请指定外部配置文件")
    System.exit(1)
  }
  //从外部文件加载properties文件信息
  PropertiesUtil.load(args(0))
  //初始化spark
  val spark = DataBaseConfig.initSpark

  import spark.implicits._

  //初始化数据库配置
  val config = DataBaseConfig(spark)
  //读取kafka流,对JSON转为DataFrame
  val inputDS = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", DataBaseConfig.CONFIG_KAFKA_SERVER)
    .option("subscribe", DataBaseConfig.CONFIG_KAFKA_TOPIC)
    .load().selectExpr("CAST(value AS STRING)")
    .as[String]
    .select(from_json($"value", DataBaseConfig.SCHEMA).as("data")).select("data.*")
  //对每个字段进行正则过滤和范围替换
  val cleanedDS = config.filter(inputDS)
  //写入kudu数据库
  cleanedDS.writeStream
    .outputMode(OutputMode.Append)
    .format("kudu")
    .option("checkpointLocation", "/tmp/temporary-" + UUID.randomUUID.toString)
    .start().awaitTermination()
}
