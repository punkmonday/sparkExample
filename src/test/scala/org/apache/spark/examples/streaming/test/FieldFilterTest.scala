package org.apache.spark.examples.streaming.test

import org.apache.spark.examples.streaming.utils.SparkUtil._
import org.apache.spark.internal.Logging

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


}
