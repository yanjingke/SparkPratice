package cn.spark.day5

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object StateFulWordCount {
 val update=(iter:Iterator[(String,Seq[Int],Option[Int])])=>{
   iter.map{case (word,current_Count,history_Count)=>
     print(current_Count.toBuffer)
     (word,current_Count.sum+history_Count.getOrElse(0))}
 }
   def main(args: Array[String]): Unit = {
     LoggerLevels.setStreamingLogLevels()
     val conf=new SparkConf().setAppName("StateFulWordCount").setMaster("local[2]")
     val sc=new SparkContext(conf)
     sc.setCheckpointDir("E://ck")
     val scc=new StreamingContext(sc,Seconds(5))
     val ds=scc.socketTextStream("172.20.134.48",8888)
     val result=ds.flatMap(_.split(" ")).map((_,1)).updateStateByKey(update,new HashPartitioner(sc.defaultParallelism),true)
     result.print()
     scc.start()
     scc.awaitTermination()
   }
}
