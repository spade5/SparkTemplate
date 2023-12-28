package name.spade5

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.streaming._

object Main {
  def main(args: Array[String]): Unit = {
//    conf.set("spark.executor.instances", "3")
//    conf.set("spark.executor.cores", "1")
//    conf.set("spark.streaming.concurrentJobs", "1")
    val sparkConf = new SparkConf().setAppName("RDDPartitionInfo").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // 这里假设你已经有一个DStream对象，例如lines，可以根据实际情况替换
    val lines = ssc.socketTextStream("localhost", 9999)

    // 获取DStream的第一个RDD
    lines.foreachRDD { rdd =>
      // 获取RDD的分区数量
      val numPartitions = rdd.getNumPartitions

      // 将每个分区的数据收集到数组中
      val partitionData = rdd.glom().collect()

      println("====================== RDD Partition Info =======================")
      println(s"Number of Partitions: $numPartitions")

      // 打印每个分区的数据量
      partitionData.zipWithIndex.foreach { case (data, partitionIndex) =>
        println(s"Partition $partitionIndex: ${data.length} elements")
      }
    }

    val wordCounts = lines.map((_, 1)).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTerminationOrTimeout(3000 * 1000)
    ssc.stop()
  }
}
