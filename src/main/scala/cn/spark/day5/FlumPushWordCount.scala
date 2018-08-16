package cn.spark.day5

import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf}

object FlumPushWordCount {
  def main(args: Array[String]): Unit = {
    val host = args(0)
    val port = args(1).toInt
    LoggerLevels.setStreamingLogLevels()
    val conf = new SparkConf().setAppName("FlumPushWordCount").setMaster("local[2]")
    //val sc=new SparkContext(conf)
    val ssc = new StreamingContext(conf, Seconds(5))
    val flumeStreaming = FlumeUtils.createStream(ssc, host, port)
    val words = flumeStreaming.flatMap(x => new String(x.event.getBody().array()).split(" ")).map((_, 1))
    val result = words.reduceByKey(_ + _)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
