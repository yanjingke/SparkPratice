package cn.spark

import org.apache.spark.{SparkConf, SparkContext}

object TestMostTest {
  val func = (index: Int, iter: Iterator[ String]) => {
    iter.toList.map(x => "[partID:" +  index + ", val: " + x + "]").iterator
  }
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName(" WordCountWeakStrong").setMaster("local")
    val sc=new SparkContext((conf))
//   val rdd= sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1))
//    val rdd3=rdd.combineByKey(x=>x,(a:Int,b:Int)=>a+b,(m:Int,n:Int)=>m+n)
//    //println(rdd3.collect().toBuffer);
//    val rdd4=rdd.combineByKey(x=>x+10,(a:Int,b:Int)=>a+b,(m:Int,n:Int)=>m+n)
    //println(rdd4.collect().toBuffer);
//    val rdd5=sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
//    val rdd6=sc.parallelize(List(1,1,2,2,2,1,2,2,2), 3)
//    val rdd7=rdd6.zip(rdd5)
//    println(rdd7.collect().toBuffer);
//    val rdd8=rdd7.combineByKey(List(_),(x:List[String],y:String)=>x:+y,(m:List[String],n:List[String])=>m++n)
//    println(rdd8.collect().toBuffer);
//    val rdd1=sc.parallelize((List(("a",1),("b",2),("c",2),("c",1))))
//    println(rdd1.countByValue().toBuffer);
//    val rdd1=sc.parallelize(List(("e","2 1"),("a","1 3"),("b","1 3"),("c","1 3"),("d","1 3")))
//    val rdd4=  rdd1.flatMapValues(_.split(" "))
//    println(rdd4.collect.toBuffer);
//    val rdd1=sc.parallelize(List("dog","wolf","cat","bear","das","dsad"),2)
    //
    //    println(rdd1.mapPartitionsWithIndex(func).collect.toBuffer)
    //    val rdd2=rdd1.map(x=>(x.length,x))
    //    val rdd3 = rdd2.foldByKey("1+")(_+_)
    //    println(rdd3.collect.toBuffer);
    //    sc.stop()
//    val rdd1=sc.parallelize(List(1,2,3,4,5,6),2)
//     rdd1.foreachPartition(x=>println(x.reduce(_+_)))
//    val rdd1=sc.parallelize(List(("e1"),("a1"),("b1"),("c1"),("d1")))
//    val rdd2=rdd1.keyBy(_(0))
//val rdd2 = sc.parallelize(1 to 10, 10)
//    val rdd1= rdd2.coalesce(2, false)
//    //rdd1.partitions.length
//    println( rdd1.partitions.length);
val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
    val b = a.keyBy(_.length)
    val c = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
    val d = c.keyBy(_.length)
    val ce=b.join(d)
    println( ce.collect().toBuffer);
  }
}

