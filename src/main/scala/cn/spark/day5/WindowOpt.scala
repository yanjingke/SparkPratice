package cn.spark.day5


import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}

object WindowOpt {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val conf = new SparkConf().setAppName("WindowOpt").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Milliseconds(5000))
    val line = ssc.socketTextStream("172.20.134.48", 8888)
    val windowsWordCount = line.flatMap(_.split(" ")).map((_, 1)).reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(15), Seconds(10))
    windowsWordCount.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
