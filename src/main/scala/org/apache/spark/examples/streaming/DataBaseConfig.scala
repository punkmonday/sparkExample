package org.apache.spark.examples.streaming

import org.apache.spark.examples.streaming.DataBaseConfig.{CONFIG_FIELD, CONFIG_RANGE, collectAsMap}
import org.apache.spark.examples.streaming.utils.{PropertiesUtil, StructTypeConverter}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{trim, typedLit, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataBaseConfig(spark: SparkSession) extends Logging {

  /**
    * 根据表名生成DataFrame
    *
    * @param tableName 表名
    * @return
    */
  def loadDF(tableName: String): DataFrame = {
    spark.read
      .format(DataBaseConfig.CONFIG_SQL_FORMAT)
      .option("url", DataBaseConfig.CONFIG_SQL_URL)
      .option("driver", DataBaseConfig.CONFIG_SQL_DRIVER)
      .option("dbtable", tableName)
      .option("user", DataBaseConfig.CONFIG_SQL_USER)
      .option("password", DataBaseConfig.CONFIG_SQL_PASSWORD)
      .load()
  }

  def collectAsRangeMap(field: String): scala.collection.Map[String, String] = {
    val sql =
      s"""select r.FieldRangeValue, r.FieldRange from $CONFIG_FIELD f, $CONFIG_RANGE r
         |where f.FieldRange = r.FieldRangeType
         |and f.FieldName = '$field'
       """.stripMargin
    spark.sparkContext.broadcast(collectAsMap(spark.sql(sql), "FieldRangeValue", "FieldRange")).value
  }

  def rangeFilter(dataFrame: DataFrame, field: String): DataFrame = {
    val rangeMap = collectAsRangeMap(field)
    val updateCol = udf((fieldName: String, map: Map[String, String]) => DataBaseConfig.convertRange(fieldName, map))
    if (rangeMap.nonEmpty) {
      dataFrame.withColumn(field, updateCol(dataFrame.col(field), typedLit(rangeMap)))
    } else {
      dataFrame
    }
  }

  def filter(dataFrame: DataFrame): DataFrame = {
    var filteredDataFrame = dataFrame
    dataFrame.columns.foreach(field => {
      filteredDataFrame = rangeFilter(DataBaseConfig.regularFilter(filteredDataFrame, field), field)
    })
    filteredDataFrame
  }
}

object DataBaseConfig {
  private val CONFIG_APP_NAME = PropertiesUtil.getProperty("config.app.name")
  private val CONFIG_APP_MASTER = PropertiesUtil.getProperty("config.app.master")
  private val CONFIG_SQL_FORMAT = PropertiesUtil.getProperty("config.sql.format")
  private val CONFIG_SQL_URL = PropertiesUtil.getProperty("config.sql.url")
  private val CONFIG_SQL_DRIVER = PropertiesUtil.getProperty("config.sql.driver")
  private val CONFIG_SQL_USER = PropertiesUtil.getProperty("config.sql.username")
  private val CONFIG_SQL_PASSWORD = PropertiesUtil.getProperty("config.sql.password")
  private val CONFIG_TABLE_NAME = PropertiesUtil.getProperty("config.sql.table.name")
  private val CONFIG_TABLE = PropertiesUtil.getProperty("config.sql.table")
  private val CONFIG_FIELD = PropertiesUtil.getProperty("config.sql.field")
  private val CONFIG_RANGE = PropertiesUtil.getProperty("config.sql.range")

  val CONFIG_KAFKA_SERVER: String = PropertiesUtil.getProperty("config.kafka.server")
  val CONFIG_KAFKA_TOPIC: String = PropertiesUtil.getProperty("config.kafka.topic")
  val CONFIG_KUDU_URL: String = PropertiesUtil.getProperty("config.kudu.url")
  val CONFIG_KUDU_TABLE: String = PropertiesUtil.getProperty("config.kudu.table")
  var SCHEMA: StructType = _
  var REGULAR_MAP: collection.Map[String, String] = _


  def apply(spark: SparkSession): DataBaseConfig = {
    val config = new DataBaseConfig(spark)
    config.loadDF(CONFIG_TABLE).createOrReplaceTempView(CONFIG_TABLE)
    config.loadDF(CONFIG_FIELD).createOrReplaceTempView(CONFIG_FIELD)
    config.loadDF(CONFIG_RANGE).createOrReplaceTempView(CONFIG_RANGE)
    val sql =
      s"""select f.FieldName,f.DataType,f.IsEmpty,f.IsGather,f.Regular,f.FieldRange
         |from $CONFIG_FIELD f left join $CONFIG_TABLE t on f.TableId = t.Id
         |where f.IsGather = true and t.TableName='$CONFIG_TABLE_NAME'""".stripMargin
    val dataFrame = spark.sql(sql)
    SCHEMA = spark.sparkContext.broadcast(StructTypeConverter.convert(dataFrame)).value
    REGULAR_MAP = spark.sparkContext.broadcast(collectAsMap(dataFrame, "FieldName", "Regular")).value
    config
  }

  /**
    * 初始化spark
    *
    * @return
    */
  def initSpark: SparkSession = {
    SparkSession
      .builder()
      .appName(CONFIG_APP_NAME)
      .master(CONFIG_APP_MASTER)
      .getOrCreate()
  }

  def collectAsMap(df: DataFrame, key: String, value: String): scala.collection.Map[String, String] = {
    df.select(key, value).rdd
      .map(row => (row.getAs[String](key), row.getAs[String](value)))
      .collectAsMap()
  }

  def convertRange(filterRange: String, map: scala.collection.Map[String, String]): String = {
    val filteredMap = map.filter(_._1.split(",").contains(filterRange))
    if (filteredMap.isEmpty) filterRange else filteredMap.values.head
  }

  def regularFilter(dataFrame: DataFrame, field: String): DataFrame = {
    if (DataBaseConfig.REGULAR_MAP(field) != null && DataBaseConfig.REGULAR_MAP(field).nonEmpty) {
      dataFrame.filter(trim(dataFrame.col(field)).rlike(DataBaseConfig.REGULAR_MAP(field)))
    } else {
      dataFrame
    }
  }
}