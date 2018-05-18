package org.apache.spark.examples.streaming

/**
  * 此代码需要修改使用spark-streaming-kafka-0-8_2.11依赖
  */
object DirectKafkaWordCount {

  //  val kuduMaster = "cdh-master:7051"
  //  val client = new KuduClient.KuduClientBuilder(kuduMaster).build()
  //  val tableName = "impala::jtdb.test"
  //  val table = client.openTable(tableName);
  //  val session = client.newSession();
  //
  //  def main(args: Array[String]) {
  //    StreamingExamples.setStreamingLogLevels()
  //    val brokers = "cdh-master:9092"
  //    val topics = "TestTopic9"
  //
  //    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("yarn-client")
  //    val ssc = new StreamingContext(sparkConf, Seconds(2))
  //
  //    val topicsSet = topics.split(",").toSet
  //    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
  //    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
  //
  //    val lines = messages.map(_._2)
  //    val words = lines.flatMap(_.split(","))
  //    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
  //    wordCounts.print()
  //
  //    //"'2018-10-13 16:33:27'","241","Test increment Data"
  //    messages.foreachRDD {
  //      pricesRDD =>
  //        val x = pricesRDD.count
  //
  //        if (x > 0) {
  //          for (line <- pricesRDD.collect.toArray) {
  //            //(null,"'2018-10-12 15:33:27'","233","Test increment Data")
  //            println(line)
  //            var createTime = line._2.split(',').view(0).toString()
  //            createTime = createTime.substring(2, createTime.length() - 2)
  //            var id = line._2.split(',').view(1).toString()
  //            id = id.substring(1, id.length() - 1)
  //            var msg = line._2.split(',').view(2).toString()
  //            msg = msg.substring(1, msg.length() - 1)
  //            val insert = table.newInsert();
  //            val row = insert.getRow();
  //            row.addInt(0, id.toInt);
  //            row.addString(1, msg);
  //            row.addString(2, createTime)
  //            session.apply(insert);
  //          }
  //        }
  //    }
  //
  //    //val lines = messages.map(_._2)
  //    //lines.print()
  //
  //    //val words = lines.flatMap(_.split(","))
  //    //val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
  //    //wordCounts.print()
  //
  //    ssc.start()
  //    ssc.awaitTermination()
  //  }
}
