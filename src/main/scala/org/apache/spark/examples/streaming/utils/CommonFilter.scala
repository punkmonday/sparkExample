package org.apache.spark.examples.streaming.utils

import org.apache.spark.sql.{DataFrame, SparkSession}


object CommonFilter {

  def filter(filterRange: String, map: scala.collection.Map[String, String]): String = {
    val filteredMap = map.filter(_._1.split(",").contains(filterRange))
    if (filteredMap.isEmpty) filterRange else filteredMap.values.head
  }

  def filter(df: DataFrame, columnName: String, regEx: String): DataFrame = {
    df.filter(df.col(columnName).rlike(regEx))
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local[*]").appName("StructTypeConverter").getOrCreate()

    val fieldRangeInfo = spark.read
      .format(PropertiesUtil.getProperty("config.sql.format"))
      .option("url", PropertiesUtil.getProperty("config.sql.url"))
      .option("dbtable", "fieldrangeinfo")
      .option("user", PropertiesUtil.getProperty("config.sql.username"))
      .option("password", PropertiesUtil.getProperty("config.sql.password"))
      .load()

    fieldRangeInfo.createOrReplaceTempView("rangeinfo")

    val rangedf = spark.sql("select FieldRangeValue,FieldRange from rangeinfo where FieldRangeType = '区域'")
      .rdd.map(row => (row.getAs("FieldRangeValue").toString, row.getAs("FieldRange").toString))
      .collectAsMap()

    rangedf.foreach(println)
    println(filter("昆明", rangedf))
    println(filter("大理", rangedf))
    println(filter("丽江", rangedf))
    println(filter("N", rangedf))

    val dd = filter(fieldRangeInfo, "FieldRange", "[7]")
    dd.show

  }
}
