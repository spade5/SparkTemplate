package name.spade5

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FileWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.textFileStream("/workspace/input/")
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map((_, 1)).reduceByKey(_ + _)

    wordCounts.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        rdd.saveAsTextFile("/workspace/output/" + System.currentTimeMillis() + ".txt")
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
