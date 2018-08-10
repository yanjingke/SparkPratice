package cn.spark

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

object LogPageVeiwCount {
  def main(args: Array[String]): Unit = {
    val conf= new SparkConf().setAppName("LogPageVeiwCount").setMaster("local")
    val sc=new SparkContext((conf))
    //方法1
//    val urlandTime=sc.textFile("E:\\itcast.log").map(x=>{
//      val arr=x.split("\t")
//      (arr(1),arr(0))
//    })
//   val hostAndTime= urlandTime.map(x=>{
//      val url=new URL( x._1).getHost
//      (url,x._2)
//    })
//    val hostAdnTimes=hostAndTime.groupByKey()
//    val urlAndCount=hostAdnTimes.map(x=>{
//     val count= x._2.toList.size
//      (x._1,count)
//    })
//  println(urlAndCount.collect().toBuffer)
    //方法2
//从数据库中加载规则
    val arr = Array("java.itcast.cn", "php.itcast.cn", "net.itcast.cn")
    val urlAndMap=sc.textFile("E:\\itcast.log").map(
      x=>{
      val urlAndTime=  x.split("\t")
        (urlAndTime(1),1)
      })
    val urlAndCount=urlAndMap.reduceByKey(_+_)
   val hostAndUrlAndCount= urlAndCount.map(x=>{
      val host=new URL( x._1).getHost
      (host,x._1,x._2)
    })
    val count= urlAndCount.map(x=>{
      val host=new URL( x._1).getHost
      (host,x._2)
    }).reduceByKey(_+_)
    println(count.collect().toBuffer)
    for(a<-arr){
      val rdd3=hostAndUrlAndCount.filter(a==_._1)
     // println(rdd3.collect().toBuffer)
    val rdd4=  rdd3.sortBy(_._3,false).take(3)
      println(rdd4.toBuffer)
    }

    //println(hostAndUrlAndCount.collect().toBuffer)
  }

}
