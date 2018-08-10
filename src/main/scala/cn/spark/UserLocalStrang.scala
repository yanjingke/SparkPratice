package cn.spark

import org.apache.spark.{SparkConf, SparkContext}

object UserLocalStrang {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("UserLocalStrang").setMaster("local")
    val sc= new SparkContext(conf)
    val baseAndTime= sc.textFile("E:\\通过基站信息计算家庭地址和工作地址\\Testdata").map(line=>{
      val arr=line.split(",")
      val time=if (arr(3)=="1") -arr(1).toLong else  arr(1).toLong
     // (((arr(2),(arr(0)),time))
        ((arr(2),arr(0)),time)
    }
    )
   val baseAndArea= sc.textFile("E:\\通过基站信息计算家庭地址和工作地址\\loc_info.txt").map(line=>{
      val arr=line.split(",")
      (arr(0),(arr(1),arr(2)))
    })
//    val baseJoinArea=baseAndTime.join(baseAndArea)
    ////    println(baseJoinArea.collect().toBuffer)
   val rdd2= baseAndTime.reduceByKey(_+_).map(t=>{
      (t._1._1,(t._1._2,t._2))
    }).join(baseAndArea)
      val rdd3=rdd2.map(x=>{
        (x._1,x._2._1._1,x._2._1._2,x._2._2)
      })
    val rdd4=rdd3.groupBy(_._2).mapValues(x=>{
      x.toList.sortBy(_._3).take(3)
    })
    println(rdd4.collect().toBuffer)
  }
}
