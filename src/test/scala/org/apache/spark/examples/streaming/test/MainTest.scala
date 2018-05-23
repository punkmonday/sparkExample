package org.apache.spark.examples.streaming.test

import java.util.UUID

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.examples.streaming.test.FieldFilterTest.registerDF
import org.apache.spark.examples.streaming.utils.{CommonFilter, PropertiesUtil, StructTypeConverter}
import org.apache.spark.sql.functions.{from_json, typedLit, udf}
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}

object MainTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.
      master("local[*]")
      .appName("StructuredStreamingTest")
      .getOrCreate
    import spark.implicits._

    val kuduContext = new KuduContext(PropertiesUtil.getProperty("config.kudu.url"), spark.sparkContext)

    registerDF(spark, "gathertables").createOrReplaceTempView("table")
    registerDF(spark, "gatherfieldset").createOrReplaceTempView("fieldSet")
    registerDF(spark, "fieldrangeinfo").createOrReplaceTempView("range")

    val tableName = "t_jc_cljbxx"
    val sql =
      s"""
         |select
         |f.FieldName,
         |f.IsGather,
         |f.DataType,
         |f.IsEmpty,
         |f.Regular,
         |f.FieldRange
         |from fieldSet f,table t
         |where f.TableId = t.Id
         |and f.IsGather = true
         |and t.TableName = '$tableName'
      """.stripMargin
    val fieldSet = spark.sql(sql)

    val regularMap = fieldSet.select("FieldName", "Regular").rdd
      .map(row => (row.getAs[String]("FieldName"), row.getAs[String]("Regular")))
      .collectAsMap()

    val scheme = StructTypeConverter.convert(fieldSet)

    val inputDS = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      //      .option("startingOffsets", "earliest")
      .load().selectExpr("CAST(value AS STRING)")
      .as[String]
      .select(from_json($"value", scheme).as("data")).select("data.*")

    var cleanDS = inputDS.as("copy")

    cleanDS.columns.foreach(field => {
      if (!regularMap(field).isEmpty) {
        cleanDS = cleanDS.filter(cleanDS.col(field).rlike(regularMap(field)))
      }
    })

    val code = (field: String, map: Map[String, String]) => {
      CommonFilter.filter(field, map)
    }
    val updateCol = udf(code)

    cleanDS.columns.foreach(fieldSet => {
      val df = spark.sql(s"select * from fieldSet f, range r where f.FieldRange = r.FieldRangeType and f.FieldName = '$fieldSet'")
      if (df.collect().length > 0) {
        val map = df.rdd.map(row => (row.getAs("FieldRangeValue").toString, row.getAs("FieldRange").toString)).collectAsMap()
        cleanDS = cleanDS.withColumn(fieldSet, updateCol(cleanDS.col(fieldSet), typedLit(map)))
      }
    })

    cleanDS.writeStream
      .outputMode("append")
      .format("console")
      .option("checkpointLocation", "/tmp/temporary-" + UUID.randomUUID.toString)
      .foreach(new ForeachWriter[Row] {
        override def open(partitionId: Long, version: Long): Boolean = true

        override def process(value: Row): Unit = {
          val df = cleanDS.sqlContext.createDataFrame(cleanDS.sparkSession.sparkContext.parallelize(Seq(value)), scheme)
          kuduContext.upsertRows(df, PropertiesUtil.getProperty("config.kudu.table"))
        }

        override def close(errorOrNull: Throwable): Unit = {}
      })
      .start().awaitTermination()
  }

}