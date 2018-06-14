package org.apache.spark.examples.streaming.test

import org.apache.kudu.spark.kudu._
import org.apache.spark.examples.streaming.DataBaseConfig
import org.apache.spark.examples.streaming.utils.PropertiesUtil
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{StructField, StructType}

object DataBaseFilterTest extends App {
  PropertiesUtil.load("/home/fxb/config.properties")
  val spark = DataBaseConfig.initSpark

  import spark.implicits._

  val filter = DataBaseConfig(spark)
  val seq = Seq("{\"ID\":123,\"CPHM\":\"云A123\", \"CPXH\":\"test\",\"CLCP\":\"test\",\"SSDQ\":\"昆明市\"}"
    , "{\"ID\":123,\"CPHM\":\"云A12345\", \"CPXH\":\"test\",\"CLCP\":\"test\",\"SSDQ\":\"昆明市\"}"
    , "{\"ID\":123,\"CPHM\":\"A45678\", \"CPXH\":\"test\",\"CLCP\":\"test\",\"SSDQ\":\"昆明市\"}"
    , "{\"ID\":123,\"CPHM\":null, \"CPXH\":null,\"CLCP\":null,\"SSDQ\":\"null\"}"
    , "{\"ID\":123,\"CPHM\":null, \"CPXH\":null,\"CLCP\":null,\"SSDQ\":null}"
    , "{\"ID\":123,\"CPHM\":\"\", \"CPXH\":\"\",\"CLCP\":\"\",\"SSDQ\":\"\"}"
    , "{\"ID\":123,\"CPHM\":\"123\"}"
    , "{\"ID\":123}")
  var df = spark.createDataset(seq).selectExpr("CAST(value AS STRING)")
    .as[String]
    .select(from_json($"value", DataBaseConfig.SCHEMA).as("data")).select("data.*")
  df.foreach(row => println(row))
  println(s"before: ${df.count}")
  println("----------------------------")
  val cleanedDf = filter.filter(df)
  cleanedDf.foreach(row => println(row))
  println(s"after: ${cleanedDf.count}")
  addBatch(1L, cleanedDf)
  spark.stop

  def addBatch(batchId: Long, data: DataFrame): Unit = {
    println("-------------------------------------------")
    println(s"addBatch($batchId)")
    val startTime = System.currentTimeMillis
    val structFields = data.schema.map(s => StructField(s.name.toLowerCase, s.dataType, s.nullable, s.metadata)).toArray
    val dataFrame = data.sparkSession.createDataFrame(data.sparkSession.sparkContext.parallelize(data.collect()), new StructType(structFields))
    dataFrame.write.options(Map("kudu.master" -> DataBaseConfig.CONFIG_KUDU_URL, "kudu.table" -> DataBaseConfig.CONFIG_KUDU_TABLE)).mode("append").kudu
    val endTime = System.currentTimeMillis
    println(s"insert ${dataFrame.count} rows into ${DataBaseConfig.CONFIG_KUDU_TABLE} in ${(endTime - startTime) / 1000} s.")
    println("-------------------------------------------")
  }
}