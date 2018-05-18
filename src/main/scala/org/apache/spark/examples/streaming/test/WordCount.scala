package org.apache.spark.examples.streaming.test

import org.apache.spark.rdd.RDD

object WordCount extends Serializable {
  def count(lines: RDD[String]): RDD[(String, Int)] = {
    val rdd = lines.flatMap(line => line.split("\\s")).map(word => (word, 1)).reduceByKey(_ + _)
    rdd
  }
}