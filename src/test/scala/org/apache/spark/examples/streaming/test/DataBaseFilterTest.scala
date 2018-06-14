package org.apache.spark.examples.streaming.test

import org.apache.spark.examples.streaming.DataBaseConfig
import org.apache.spark.examples.streaming.utils.PropertiesUtil
import org.apache.spark.sql.functions.from_json

object DataBaseFilterTest extends App {

  PropertiesUtil.load("/home/fxb/config.properties")

  val spark = DataBaseConfig.initSpark

  import spark.implicits._

  val filter = DataBaseConfig(spark)

  val seq = Seq("{\"ID\":123,\"CPHM\":\"云A123\", \"CPXH\":\"test\",\"CLCP\":\"test\",\"SSDQ\":\"昆明市\"}"
    , "{\"ID\":123,\"CPHM\":\"云A12345\", \"CPXH\":\"test\",\"CLCP\":\"test\",\"SSDQ\":\"昆明市\"}"
    , "{\"ID\":123,\"CPHM\":\"A45678\", \"CPXH\":\"test\",\"CLCP\":\"test\",\"SSDQ\":\"昆明市\"}"
    , "{\"ID\":333,\"CPHM\":null, \"CPXH\":null,\"CLCP\":null,\"SSDQ\":\"null\"}")
  var df = spark.createDataset(seq).selectExpr("CAST(value AS STRING)")
    .as[String]
    .select(from_json($"value", DataBaseConfig.SCHEMA).as("data")).select("data.*")
  df.foreach(row => println(row))
  println("----------------------------")
  val cleanedDf = filter.filter(df)
  cleanedDf.foreach(row => println(row))
}