package name.spade5

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("yarn-client")
    conf.set("spark.executor.instances", "3")
    conf.set("spark.executor.cores", "1")
    conf.set("spark.streaming.concurrentJobs", "1")

    val ssc = new StreamingContext(conf, Seconds(10))

    val lines = ssc.socketTextStream("hadoop1", 9999).repartition(1)

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map((_, 1)).reduceByKey(_ + _)

    wordCounts.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        rdd.saveAsTextFile("/workspace/output/" + System.currentTimeMillis())
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
