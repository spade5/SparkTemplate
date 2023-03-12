package name.spade5

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]) = {
    System.setProperty("HADOOP_USER_NAME", "root")

    val conf = new SparkConf().setAppName("RealtimeEtl").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val rdd = spark.sparkContext.textFile("mockDAUData.txt")

    val res = rdd.map((line: String) => {
      val splits = line.split(",")
      ((splits(0), splits(1)), 1)
    }).reduceByKey((a, b) => 1) //日期，用户 去重
      .map((tuple) => (tuple._1._1, 1))  //统计去重的用户数
      .reduceByKey(_+_)

    res.foreach(println)
    res.saveAsTextFile("hdfs://hadoop0:9000/user/root/DAU_data")
  }
}
