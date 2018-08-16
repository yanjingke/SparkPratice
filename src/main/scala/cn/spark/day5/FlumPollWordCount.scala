package cn.spark.day5

import java.net.InetSocketAddress

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FlumPollWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FlumPollWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val address = Seq(new InetSocketAddress("hadoop", 8888))
    val flumeStreaming = FlumeUtils.createPollingStream(ssc, address, StorageLevel.MEMORY_AND_DISK)
    val words = flumeStreaming.flatMap(x => new String(x.event.getBody().array()).split(" ")).map((_, 1))
    val results = words.reduceByKey(_ + _)
    results.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
