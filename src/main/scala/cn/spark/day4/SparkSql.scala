package cn.spark.day4

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("SparkSql")
    val sc=new SparkContext(conf)
    val sqlContext=new SQLContext(sc)
    val linRdd=sc.textFile(args(0)).map(_.split(","))
    val personRDD=linRdd.map(x=>Person(x(0).toInt,x(1).toString,x(2).toInt))
    //导入隐式转换，如果不导入无法将RDD转换成DataFrame
    //将RDD转换成DataFrame
    import sqlContext.implicits._
    val personDF=personRDD.toDF
    personDF.registerTempTable("t_person")
   // val df=sqlContext.sql("select * from t_person order by age desc limit 2")
    val df = sqlContext.sql("select * from t_person order by age desc limit 2")
    //将结果以JSON的方式存储到指定位置
      df.write.json(args(1))
    //停止Spark Context

    sc.stop()

  }
}
case  class  Person(id:Int,name:String,age:Int)