package org.apache.spark.examples.streaming

import java.util.UUID

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.examples.streaming.utils.{CommonFilter, PropertiesUtil}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}

object FieldFilter extends Logging {

  def main(args: Array[String]) {

    val spark = initSpark

    import spark.implicits._

    registerDF(spark, "gatherfieldset").createOrReplaceTempView("fieldSet")

    val fieldRegEx = spark.sql("select Regular from fieldSet where FieldName = 'CPHM'").collect().last.getAs[String](0)

    val fieldRange = spark.sql("select FieldRange from fieldSet where FieldName = 'SSDQ'").collect().last.getAs[String](0)

    registerDF(spark, "fieldrangeinfo").createOrReplaceTempView("range")

    val rangeMap = spark.sql(s"select FieldRangeValue,FieldRange from range where FieldRangeType = '$fieldRange'")
      .rdd.map(row => (row.getAs("FieldRangeValue").toString, row.getAs("FieldRange").toString))
      .collectAsMap()

    val inputDS = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", PropertiesUtil.getProperty("config.kafka.server"))
      .option("subscribe", PropertiesUtil.getProperty("config.kafka.topic"))
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    val cleanDS = inputDS.map(line => {
      val fields = line.replaceAll("\"", "").split(",")
      FIELD(fields(0).toInt, fields(1), fields(2))
    })


    val filteredDS = cleanDS.filter(cleanDS.col("CPHM").rlike(fieldRegEx))
      .map(line => {
        FIELD(line.ID, line.CPHM, CommonFilter.filter(line.SSDQ, rangeMap))
      })

    //ETL后保存到kudu
    val kuduContext = new KuduContext(PropertiesUtil.getProperty("config.kudu.url"), spark.sparkContext)

    filteredDS.writeStream
      .format("console")
      .foreach(new ForeachWriter[FIELD] {

        override def open(partitionId: Long, version: Long): Boolean = {
          true
        }

        override def process(value: FIELD): Unit = {


          val spark = initSpark

          val row = Row(value.ID, value.CPHM, value.SSDQ)

          val valRdd = spark.sparkContext.parallelize(Seq(row))

          val schema = StructType(Array(
            StructField("id", IntegerType),
            StructField("cphm", StringType),
            StructField("ssdq", StringType)
          ))

          val valDF = spark.createDataFrame(valRdd, schema)

          kuduContext.upsertRows(valDF, PropertiesUtil.getProperty("config.kudu.table"))
        }

        override def close(errorOrNull: Throwable): Unit = {
        }

      })
      .option("checkpointLocation", "/tmp/temporary-" + UUID.randomUUID.toString)
      .start().awaitTermination()
  }

  case class FIELD(ID: Int, CPHM: String, SSDQ: String)

  /**
    * 根据表名生成DataFrame
    *
    * @param spark     sparkSession
    * @param tableName 表名
    * @return
    */
  def registerDF(spark: SparkSession, tableName: String): DataFrame = {
    spark.read
      .format(PropertiesUtil.getProperty("config.sql.format"))
      .option("url", PropertiesUtil.getProperty("config.sql.url"))
      .option("dbtable", if (tableName.isEmpty) PropertiesUtil.getProperty("config.sql.table") else tableName)
      .option("user", PropertiesUtil.getProperty("config.sql.username"))
      .option("password", PropertiesUtil.getProperty("config.sql.password"))
      .load()

  }

  /**
    * 初始化spark
    *
    * @return
    */
  def initSpark: SparkSession = {
    SparkSession
      .builder()
      .appName(PropertiesUtil.getProperty("config.app.name"))
      .master(PropertiesUtil.getProperty("config.app.master"))
      .getOrCreate()
  }

}
