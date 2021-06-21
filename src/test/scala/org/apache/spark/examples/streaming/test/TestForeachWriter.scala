package org.apache.spark.examples.streaming.test

import org.apache.spark.sql.ForeachWriter

object TestForeachWriter extends ForeachWriter {
  override def open(partitionId: Long, version: Long): Boolean = true

  override def process(value: Nothing): Unit = {
    //logic
  }

  override def close(errorOrNull: Throwable): Unit = {}
}