//此代码需要修改使用spark-streaming-kafka-0-8_2.11依赖
//package org.apache.spark.examples.streaming
//
//import kafka.serializer.StringDecoder
//import org.apache.kudu.client.KuduClient
//import org.apache.spark.examples.streaming.utils.{PropertiesUtil, StructTypeConverter}
//import org.apache.spark.sql
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//import scala.util.matching.Regex
//
//object StreamingKafkaKuduCPHMFilter {
//
//  val kuduMaster = "cdh-master:7051"
//  val brokers = "cdh-master:9092"
//  val topics = "MysqlTestTopic"
//  val tableName = "impala::jtdb.test_cljbxx1"
//  // 车牌号码正则
//  val cphmRegex: Regex = "^[京津沪渝冀豫云辽黑湘皖鲁新苏浙赣鄂桂甘晋蒙陕吉闽贵粤青藏川宁琼使领]{1}(厂)?[A-Z]{1}(?:(?![A-Z]{4})[A-Z0-9]){4}[A-Z0-9挂学警港澳]{1}$".r
//
//  def main(args: Array[String]) {
//
//    val client = new KuduClient.KuduClientBuilder(kuduMaster).build()
//    val table = client.openTable(tableName)
//    val session = client.newSession()
//
//    //放服务器执行需要修改master为"yarn-client"
//    val spark = SparkSession.builder().appName("My App").master("local[*]").getOrCreate()
//    val sc = spark.sparkContext
//
//    val ssc = new StreamingContext(sc, Seconds(2))
//
//    val topicsSet = topics.split(",").toSet
//    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")
//    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
//
//    val gatherfieldset = registerDF(spark, "gatherfieldset")
//    gatherfieldset.show
//
//
//    messages.foreachRDD {
//      pricesRDD =>
//        val x = pricesRDD.count
//
//        if (x > 0) {
//          for (line <- pricesRDD.collect) {
//            println(line)
//            var CPHM = line._2.split(',').view(2).toString
//            CPHM = CPHM.substring(1, CPHM.length() - 1)
//
//            //如果车牌号匹配正则规则，就写入数据
//            if (cphmRegex.pattern.matcher(CPHM).matches()) {
//              var id = line._2.split(',').view(0).toString
//              id = id.substring(1, id.length() - 1)
//
//              var SSDQ = line._2.split(',').view(3).toString
//              SSDQ = SSDQ.substring(1, SSDQ.length() - 1)
//              println(SSDQ)
//
//              //              val insert = table.newInsert()
//              //              val row = insert.getRow
//              //              row.addInt(0, id.toInt)
//              //              row.addString(2, CPHM)
//              //              row.addString(3, SSDQ)
//              //              session.apply(insert)
//
//
//            }
//          }
//        }
//    }
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//
//  def registerDF(spark: SparkSession, tableName: String): sql.DataFrame = {
//    spark.read
//      .format(PropertiesUtil.getProperty("config.sql.format"))
//      .option("url", PropertiesUtil.getProperty("config.sql.url"))
//      .option("dbtable", if (tableName.isEmpty) PropertiesUtil.getProperty("config.sql.table") else tableName)
//      .option("user", PropertiesUtil.getProperty("config.sql.username"))
//      .option("password", PropertiesUtil.getProperty("config.sql.password"))
//      .load()
//
//  }
//}
