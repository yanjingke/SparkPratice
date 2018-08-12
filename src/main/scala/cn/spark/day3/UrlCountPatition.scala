package cn.spark.day3

import java.net.URL

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object UrlCountPatition {
  def main(args: Array[String]): Unit = {
      val conf=new SparkConf().setAppName("UrlCountPatition").setMaster("local")
      val sc=new SparkContext(conf)
   val countUrl= sc.textFile("E:\\itcast.log").map(x=>{
      val arr=x.split("\t")
      (arr(1),1)
    }).reduceByKey(_+_)
    val hostCount=countUrl.map(x=>{
      val host=new URL(x._1).getHost
      (host,(x._1,x._2))
    })
   // hostCount.repartition(3).saveAsTextFile("E://test")重新分区
  val rdd=hostCount.map(_._1).distinct().collect()
    val hostPartitione=new HostPartitioner(rdd)
    val rdd2=hostCount.partitionBy(hostPartitione).mapPartitions(it=>{
      it.toList.sortBy(_._2._2).reverse.take(2).iterator
    })
      rdd2.saveAsTextFile("E://test")
  }
}
class HostPartitioner(ins:Array[String])extends Partitioner{
  var count =0;
    val  map=  new mutable.HashMap[String,Int]()
  for(i<-ins){
    map+=(i->count)
    count+=1
  }
  override def numPartitions: Int =ins.length

  override def getPartition(key: Any): Int = {
    map.getOrElse(key.toString,0)
  }
}
