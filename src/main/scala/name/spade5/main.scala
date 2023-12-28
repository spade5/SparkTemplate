package name.spade5

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.PromptPartitioner
import org.apache.spark.HashPartitioner

object Main {
  def main(args: Array[String]): Unit = {
//    conf.set("spark.executor.instances", "3")
//    conf.set("spark.executor.cores", "1")
//    conf.set("spark.streaming.concurrentJobs", "1")
    val conf = new SparkConf()

    var usePrompt = false
    if (args.length > 0 && args(0) == "prompt") {
      usePrompt = true
      println("using prompt partitioner!!!")
      conf.setExecutorEnv("Partitioner","PromptPartitioner" )
    }

    var topic = "twitter"
    if (args.length > 1) {
      topic = args(1)
    }

    var maxRate = "200000"

    if (args.length > 2) {
      maxRate = args(2)
    }
    conf.set("spark.streaming.kafka.maxRatePerPartition", maxRate)

    conf.setAppName("WordCount-" + topic + "-" + maxRate)

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

    val topics = Array(topic)
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node85:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test-group",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    val kafkaDStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    val rdd0 = kafkaDStream.flatMap(_.value().split(" ")).map((_, 1))

    val rdd1 = rdd0.repartition(3)
      .map((_, 1))
      .reduceByKey(_ + _)
      .saveAsTextFiles("/home/chenhao/output/counts")

//    kafkaDStream.flatMap(line => {
//      val splits = line.value().split(",")
//      splits(3).split(" ")
//    }).map((_, 1)).reduceByKey(_ + _).saveAsTextFiles("/home/chenhao/output/counts")
    /*kafkaDStream.foreachRDD(kafkaRDD => {
      if (!kafkaRDD.isEmpty()) {
        //获取当前批次的RDD的偏移量
        val offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
        //提交当前批次的偏移量，偏移量最后写入kafka
        kafkaDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        //拿出kafka中的数据
        val words = kafkaRDD.flatMap(line => {
          val splits = line.value().split(",")
          splits(3).split(" ")
        })
//        lines.saveAsTextFile("/home/chenhao/output/lines/" + System.currentTimeMillis())
        val wordCounts = words.map((_, 1)).reduceByKey(_ + _)

        wordCounts.saveAsTextFile("/home/chenhao/output/counts/" + System.currentTimeMillis())
      } else {
        println(kafkaRDD.id, "is Empty")
        ssc.stop()
      }
    })*/

    ssc.start()
    ssc.awaitTerminationOrTimeout(3000 * 1000)
    ssc.stop()
  }
}
