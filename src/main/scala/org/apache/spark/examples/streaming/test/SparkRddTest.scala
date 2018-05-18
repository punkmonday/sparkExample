package org.apache.spark.examples.streaming.test

import org.apache.spark.SparkContext

object SparkRddTest {

  def main(args: Array[String]): Unit = {

    val sparkContext = new SparkContext("local[*]", "testAppName")

    val rdd = sparkContext.parallelize(List((1, "1"), (2, "2"), (3, "3"), (4, "4"), (5, "5"), (6, "6"), (7, "7"), (8, "8"), (9, "9"), (10, "10")))

    rdd.foreach(print)

    println("")

    rdd.filter(_._2.toInt <= 5).foreach(print)

    println("")

    rdd.map(_._2).foreach(print)

    println("")

    rdd.map("value: " + _._2).foreach(print)

    println("")

    rdd.mapValues(_.toInt + 1).foreach(print)

    println("")

    rdd.flatMap(_._2).foreach(print)

    println("")

    rdd.foreachPartition(print)
  }

}
