package org.apache.spark.examples.streaming.test

object SparkMainTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.
      master("local[*]")
      .appName("StructuredStreamingTest")
      .getOrCreate

    val tableName = "t_jc_cljbxx"

    registerDF(spark, "gathertables").createOrReplaceTempView("table")
    val table = spark.sql("select * from table")
    registerDF(spark, "gatherfieldset").createOrReplaceTempView("fieldSet")
    val fieldSet = spark.sql("select * from fieldSet f where f.IsGather = true and f.TableId = 17")
    fieldSet.show

    registerDF(spark, "fieldrangeinfo").createOrReplaceTempView("range")
    val range = spark.sql("select * from range")

    val map = spark.sql(s"select FieldRangeValue,FieldRange from range")
      .rdd.map(row => (row.getAs("FieldRangeValue").toString, row.getAs("FieldRange").toString))
      .collectAsMap()
    map.foreach(println)


    val cleangrange = spark.sql("select FieldRangeType, concat_ws(',',collect_set(FieldRangeValue)) as values from range group by FieldRangeType")
    cleangrange.show
    cleangrange.rdd.foreach(println)

    val joinedFieldSet = fieldSet.join(cleangrange, fieldSet("FieldRange") === range("FieldRangeType"), "left").select("FieldName", "FieldRangeType", "values")
    joinedFieldSet.show
  }

}