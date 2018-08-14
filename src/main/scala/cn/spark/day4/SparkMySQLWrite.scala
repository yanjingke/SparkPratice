package cn.spark.day4

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkMySQLWrite {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("SparkMySQLWrite")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val personRDD = sc.parallelize(Array("1 tom 5", "2 jerry 3", "3 kitty 6")).map(_.split(" "))
      .map(x => {
        Person(x(0).toInt, x(1).toString, x(2).toInt)
      })
    //导入隐式转换，如果不导入无法将RDD转换成DataFrame
    //将RDD转换成DataFrame
    import sqlContext.implicits._
    val personDF = personRDD.toDF()
    personDF.registerTempTable("t1_person")
    val df= sqlContext.sql("select * from t1_person order by age")
    val prop=new Properties()
    prop.put("user","root")
    prop.put("password","123")
    df.write.mode("append").jdbc("jdbc:mysql://hadoop:3306/bigdata","bigdata.person",prop)
    //停止SparkContext
    sc.stop()

  }
}

case class Person(id: Int, name: String, age: Int)
