package cn.spark.day5

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaPullWordCount {
  val updateFunc = (item: Iterator[(String, Seq[Int], Option[Int])]) => {
    item.flatMap { case (x, y, z) => Some(y.sum + z.getOrElse(0)).map(i=>{(x,i)})
    }
  }
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val Array(zkCloud, group, topics, numTreads) = args
    val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaPullWordCount")
    val scc = new StreamingContext(conf, Seconds(5))
    scc.checkpoint("e://ck2")
    //"alog-2016-04-16,alog-2016-04-17,alog-2016-04-18"
    //"Array((alog-2016-04-16, 2), (alog-2016-04-17, 2), (alog-2016-04-18, 2))"
    val topicMap = topics.split(",").map((_, numTreads.toInt)).toMap
    val data = KafkaUtils.createStream(scc, zkCloud, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)
  // val result = data.map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    val result=data.map(_._2).flatMap(_.split(" ")).map((_, 1)).updateStateByKey(updateFunc,new HashPartitioner(scc.sparkContext.defaultParallelism),true )
    result.print()
    scc.start()
    scc.awaitTermination()
  }
}
