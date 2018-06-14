package org.apache.spark.examples.streaming.provider

import org.apache.kudu.spark.kudu._
import org.apache.spark.examples.streaming.DataBaseConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}

case class KuduSink(sqlContext: SQLContext,
                    parameters: Map[String, String],
                    partitionColumns: Seq[String],
                    outputMode: OutputMode) extends Sink with Logging {

  private val KUDU_URL = DataBaseConfig.CONFIG_KUDU_URL
  private val KUDU_TABLE = DataBaseConfig.CONFIG_KUDU_TABLE

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    println("-------------------------------------------")
    println(s"addBatch($batchId)")
    val startTime = System.currentTimeMillis
    val structFields = data.schema.map(s => StructField(s.name.toLowerCase, s.dataType, s.nullable, s.metadata)).toArray
    val dataFrame = data.sparkSession.createDataFrame(data.sparkSession.sparkContext.parallelize(data.collect()), new StructType(structFields))
    dataFrame.write.options(Map("kudu.master" -> KUDU_URL, "kudu.table" -> KUDU_TABLE)).mode("append").kudu
    val endTime = System.currentTimeMillis
    println(s"insert ${dataFrame.count} rows into $KUDU_TABLE in ${(endTime - startTime) / 1000} s.")
    println("-------------------------------------------")
  }
}