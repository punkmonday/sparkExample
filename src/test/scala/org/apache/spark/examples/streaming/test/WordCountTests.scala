package org.apache.spark.examples.streaming.test

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FlatSpec}

/**
  * WrodCount的单元测试
  */
class WordCountTests extends FlatSpec with BeforeAndAfter {
  val master = "local" //sparkcontext的运行master
  var sc: SparkContext = _

  //这里before和after中分别进行sparkcontext的初始化和结束，如果是SQLContext也可以在这里面初始化
  before {
    val conf = new SparkConf()
      .setAppName("test").setMaster(master)
    sc = new SparkContext(conf)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }

  it should ("test success") in {
    //其中参数为rdd或者dataframe可以通过通过简单的手动构造即可
    val seq = Seq("the test test1", "the test", "the")
    val rdd = sc.parallelize(seq)
    val wordCounts = WordCount.count(rdd)
    //这里要collect一下，否则会报NotSerializableException
    wordCounts.collect.foreach(line => {
      line._1 match {
        case "the" => assert(line._2 == 3)
        case "test" => assert(line._2 == 2)
        case "test1" => assert(line._2 == 1)
      }
    })
  }
}