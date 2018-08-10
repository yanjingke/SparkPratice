package cn.spark

import org.apache.spark.{SparkConf, SparkContext}

object UserLocal {
  def main(args: Array[String]): Unit = {
    val conf= new SparkConf().setAppName("UserLocal").setMaster("local")
    val sc=new SparkContext(conf)
    val modolAndTime=sc.textFile("E:\\通过基站信息计算家庭地址和工作地址\\Testdata").map(x=>{
      val arr= x.split(",")
      val time=if (arr(3)=="1") -arr(1).toLong else arr(1).toLong
      (arr(0)+"_"+arr(2),time)
    })
    val modelStayTime=modolAndTime.reduceByKey(_+_)
    val rdd2=modelStayTime.map(x=>{
      val arr=x._1.split("_")
      val mobile=arr(0)
      val lac=arr(1)
      val time=x._2
      (mobile,lac,time)
    })
    val rdd3=rdd2.groupBy(_._1)
   val rdd4= rdd3.mapValues(it=>{
      it.toList.sortBy(_._3).take(3)
    })
    println(rdd4.collect().toBuffer)
    sc.stop()
  }

}
