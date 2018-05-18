package org.apache.spark.examples.streaming.test

import org.apache.spark.examples.streaming.utils.PropertiesUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

object FieldFilterTest extends Logging {

  def main(args: Array[String]) {

    val spark = initSpark

    registerDF(spark, "gathertables").createOrReplaceTempView("table")
    spark.sql("select * from table").show

    registerDF(spark, "gatherfieldset").createOrReplaceTempView("fieldSet")
    val fieldSet = spark.sql("select * from fieldSet f where f.IsGather = true")
    fieldSet.show

    registerDF(spark, "fieldrangeinfo").createOrReplaceTempView("range")
    spark.sql("select * from range r ").show

    fieldSet.foreach(row => {
      println(row)
    })


  }

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
