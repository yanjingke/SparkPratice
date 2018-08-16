package cn.spark.day5

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val conf=new SparkConf().setAppName("StreamingWordCount").setMaster("local[2]")
    val sc=new SparkContext(conf)
    val scc=new StreamingContext(sc,Seconds(5))
    val ds=scc.socketTextStream("172.20.134.48",8888)
    val result=ds.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    result.print()
    scc.start()
    scc.awaitTermination()
  }
}
