package org.apache.spark.examples.streaming.utils

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}


object StructTypeConverter {

  def convert(df: DataFrame): StructType = {
    val structFields = df.rdd.filter(row => row.getAs("IsGather").toString.toBoolean).map(row =>
      StructField(
        row.getAs("FieldName").toString,
        row.getAs("DataType").toString match {
          case "VARCHAR" => StringType
          case "BIGINT" => IntegerType
          case "INT" => IntegerType
          case "BOOLEAN" => BooleanType
          case _ => StringType
        },
        row.getAs("IsEmpty")
      )
    ).collect()

    new StructType(structFields)
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local[*]").appName("StructTypeConverter").getOrCreate()
    val df = spark.read
      .format(PropertiesUtil.getProperty("config.sql.format"))
      .option("url", PropertiesUtil.getProperty("config.sql.url"))
      .option("dbtable", PropertiesUtil.getProperty("config.sql.table"))
      .option("user", PropertiesUtil.getProperty("config.sql.username"))
      .option("password", PropertiesUtil.getProperty("config.sql.password"))
      .load()
    val schema = StructTypeConverter.convert(df)
    println(schema)
  }
}
