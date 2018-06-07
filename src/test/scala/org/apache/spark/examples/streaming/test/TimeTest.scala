package org.apache.spark.examples.streaming.test

import org.apache.spark.sql.SparkSession

object TimeTest {

  def time[T](f: => T): T = {
    val start = System.nanoTime()
    val ret = f
    val end = System.nanoTime()
    // scalastyle:off println
    println(s"Time taken: ${(end - start) / 1000 / 1000} ms")
    // scalastyle:on println
    ret
  }

  def main(args: Array[String]): Unit = {
    time(1 to 1000 by 1 toList)

    time {
      SparkSession.builder.
        master("local[*]")
        .appName("MainTest")
        .config("spark.debug.maxToStringFields", 200)
        .getOrCreate
    }
  }
}
