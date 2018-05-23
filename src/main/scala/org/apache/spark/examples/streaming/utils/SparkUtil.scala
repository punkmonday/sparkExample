package org.apache.spark.examples.streaming.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkUtil {

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

  def createDFFromTable(spark: SparkSession, tableName: String, viewName: String): Unit = {
    registerDF(spark, tableName).createOrReplaceTempView(viewName)
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

  def collectAsMap(df: DataFrame, key: String, value: String): scala.collection.Map[String, String] = {
    df.select(key, value).rdd
      .map(row => (row.getAs[String](key), row.getAs[String](value)))
      .collectAsMap()
  }
}
