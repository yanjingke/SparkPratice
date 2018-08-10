package cn.spark

import org.apache.spark.{SparkConf, SparkContext}
;

object WordCount {
  def main(args: Array[String]) {
    //创建SparkConf()并设置App名称
    val conf= new SparkConf().setAppName("WC")
//创建SparkContext，该对象是提交spark App的入口
val sc = new SparkContext(conf)
//使用sc创建RDD并执行相应的transformation和action
sc.textFile(args(0)).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_, 1).sortBy(_._2, false).saveAsTextFile(args(1))
//停止sc，结束该任务
   val rdd1=sc.parallelize(List(1,2,3,4,5,6),2)
    rdd1.mapPartitionsWithIndex((x,y)=>{y}).collect
    val func = (index: Int, iter: Iterator[Int]) => {
      iter.toList.map(x => "parID:" + index + ",val:" + x).iterator
    }
    val func1 = (index: Int, iter: Iterator[(Int)]) => {
      iter.toList.map(x => "[partID:" +  index + ", val: " + x + "]").iterator
    }
sc.stop()
}
}
