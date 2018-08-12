package cn.spark.day3

import org.apache.spark.{SparkConf, SparkContext}

object MoveTest {
  def main(args: Array[String]): Unit = {
    val conf =  new SparkConf().setAppName("MoveTest").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd=sc.textFile("E:\\电影数据\\move.txt").map(x=>{
     val arr= x.split(",")
      (arr(0),arr(2),arr(3),arr(5))
    })
    val count=rdd.count();
   val rdd2= rdd.filter(_._2.toFloat>=4)
    val goodPeoleCount=rdd2.count()
    val goodRate=goodPeoleCount.toFloat/count.toFloat
    println("好评率:"+goodRate)
       val areaCount=rdd.map(x=>{
         (x._3,1)
       }).reduceByKey(_+_).sortBy(_._2,false).take(4).map(_._1)

    println("城市分布人数:"+areaCount.toBuffer)
    val countSex=rdd.map(x=>{
      (x._4,1)
    }).reduceByKey(_+_)
    println("男女观影人数:"+countSex.collect().toBuffer)
  }
}
