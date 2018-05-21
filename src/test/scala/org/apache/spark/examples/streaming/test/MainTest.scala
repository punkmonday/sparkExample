package org.apache.spark.examples.streaming.test

import java.util.UUID

import org.apache.spark.examples.streaming.test.FieldFilterTest.registerDF
import org.apache.spark.examples.streaming.utils.{CommonFilter, StructTypeConverter}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_json, typedLit, udf}

object MainTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.
      master("local[*]")
      .appName("StructuredStreamingTest")
      .getOrCreate

    import spark.implicits._


    val tableName = "t_jc_cljbxx"

    registerDF(spark, "gathertables").createOrReplaceTempView("table")
    spark.sql("select * from table").show

    registerDF(spark, "gatherfieldset").createOrReplaceTempView("fieldSet")

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
    fieldSet.show

    val regularMap = fieldSet.select("FieldName", "Regular").rdd
      .map(row => (row.getAs[String]("FieldName"), row.getAs[String]("Regular")))
      .collectAsMap()

    val scheme = StructTypeConverter.convert(fieldSet)

    println(scheme)

    registerDF(spark, "fieldrangeinfo").createOrReplaceTempView("range")
    spark.sql("select * from fieldSet f, range r where f.FieldRange = r.FieldRangeType").show

    val map = spark.sql(s"select FieldRangeValue,FieldRange from range")
      .rdd.map(row => (row.getAs("FieldRangeValue").toString, row.getAs("FieldRange").toString))
      .collectAsMap()


    val ds = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load().selectExpr("CAST(value AS STRING)")
      .as[String]

    val df1 = ds.select(from_json($"value", scheme).as("data")).select("data.*")

    var df2 = df1.as("copy")

    df1.columns.foreach(field => {
      println(regularMap(field))
      if (!regularMap(field).isEmpty) {
        df2 = df2.filter(df2.col(field).rlike(regularMap(field)))
      }
    })

    val code = (field: String, map: Map[String, String]) => {
      CommonFilter.filter(field, map)
    }
    val updateCol = udf(code)

    df1.columns.foreach(fieldSet => {
      val sql = spark.sql("select * from fieldSet f, range r where f.FieldRange = r.FieldRangeType")
      if (sql.collect().size > 0) {
        val map = sql.rdd.map(row => (row.getAs("FieldRangeValue").toString, row.getAs("FieldRange").toString))
          .collectAsMap()
        df2 = df2.withColumn(fieldSet, updateCol(df2.col(fieldSet), typedLit(map)))
      }
    })

    df2.writeStream
      .outputMode("append")
      .format("console")
      .option("checkpointLocation", "/tmp/temporary-" + UUID.randomUUID.toString)
      .start().awaitTermination()
  }

}