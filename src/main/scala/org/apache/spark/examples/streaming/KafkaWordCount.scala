package org.apache.spark.examples.streaming

/**
  * 此代码需要修改使用spark-streaming-kafka-0-8_2.11依赖
  * Consumes messages from one or more topics in Kafka and does wordcount.
  * Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>
  * <zkQuorum> is a list of one or more zookeeper servers that make quorum
  * <group> is the name of kafka consumer group
  * <topics> is a list of one or more kafka topics to consume from
  * <numThreads> is the number of threads the kafka consumer should use
  *
  * Example:
  * `$ bin/run-example \
  *      org.apache.spark.examples.streaming.KafkaWordCount zoo01,zoo02,zoo03 \
  * my-consumer-group topic1,topic2 1`
  */
object KafkaWordCount {
  def main(args: Array[String]) {

    //    StreamingExamples.setStreamingLogLevels()
    //    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("kafka-spark-demo")
    //    val scc = new StreamingContext(sparkConf, Seconds(2))
    //    scc.sparkContext.setLogLevel("ERROR")
    //    scc.checkpoint(".") // 因为使用到了updateStateByKey,所以必须要设置checkpoint
    //    val topics = Set("TestTopic6") //我们需要消费的kafka数据的topic
    //    val brokers = "cdh-master:9092"
    //    val kafkaParam = Map[String, String](
    //      //"zookeeper.connect" -> "172.16.10.231:2181",
    //      //"group.id" -> "TestTopic.id",
    //      "metadata.broker.list" -> brokers,// kafka的broker list地址
    //      "serializer.class" -> "kafka.serializer.StringEncoder"
    //    )
    //
    //    val stream: InputDStream[(String, String)] = createStream(scc, kafkaParam, topics)
    //
    //    stream.map(_._2)      // 取出value
    //      .flatMap(_.split(",")) // 将字符串使用空格分隔
    //      .map(r => (r, 1))      // 每个单词映射成一个pair
    //      .updateStateByKey[Int](updateFunc)  // 用当前batch的数据区更新已有的数据
    //      .print() // 打印前10个数据
    //    scc.start() // 真正启动程序
    //    scc.awaitTermination() //阻塞等待
    //  }
    //
    //  val updateFunc = (currentValues: Seq[Int], preValue: Option[Int]) => {
    //    val curr = currentValues.sum
    //    val pre = preValue.getOrElse(0)
    //    Some(curr + pre)
    //  }
    //  /**
    //    * 创建一个从kafka获取数据的流.
    //    * @param scc           spark streaming上下文
    //    * @param kafkaParam    kafka相关配置
    //    * @param topics        需要消费的topic集合
    //    * @return
    //    */
    //  def createStream(scc: StreamingContext, kafkaParam: Map[String, String], topics: Set[String]) = {
    //    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](scc, kafkaParam, topics)
  }
}
