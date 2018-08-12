package cn.spark.day3

import org.apache.spark.{SparkConf, SparkContext}
object OrderContext{
  implicit val girlOrdering =new Ordering[Girl]{
    override def compare(x: Girl, y: Girl): Int = {
      if(x.faceValue>y.faceValue) 1
      else if(x.faceValue==y.faceValue)  {
        if(x.age>y.age) -1 else 1
      }else -1
    }
  }
}

object CusetomSort {
  def main(args: Array[String]): Unit = {
   val conf =  new SparkConf().setAppName("CusetomSort ").setJars(Array("D:\\hadoop\\SparkPratice\\target\\SparkPratice-1.0-SNAPSHOT.jar")).setMaster("spark://hadoop:7077").set("spark.executor.memory", "512m");//远程调用spark提交到集群
//  val conf=  new SparkConf().setAppName("CusetomSort")
    val sc=new SparkContext(conf)
    val rdd=sc.parallelize(List(("jjdajas",20,30),("sdadsa",10,39),("adssadsa",10,40)))
    import OrderContext._
   val rdd2= rdd.sortBy(x=>Girl(x._2,x._3),false)
    println(rdd2.collect.toBuffer)
    sc.stop()
  }
}
case class Girl(faceValue:Int,age:Int) extends Serializable
