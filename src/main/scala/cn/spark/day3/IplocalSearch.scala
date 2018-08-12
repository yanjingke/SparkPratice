package cn.spark.day3

import java.sql
import java.sql.{Connection, Date, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}


object IplocalSearch {
  val dataMysql = (iterator: Iterator[(String, Int)]) => {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "INSERT INTO location_info (location, counts, accesse_date) VALUES (?, ?, ?)"
    conn = DriverManager.getConnection("jdbc:mysql://hadoop:3306/bigdata", "root", "123")
    try {
      iterator.foreach(line => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, line._1)
        ps.setInt(2, line._2)
        ps.setDate(3, new Date(System.currentTimeMillis()))
        ps.executeUpdate()
      })
    } catch {
      case e: Exception => println("失败")
    } finally {
      if (ps != null)
        ps.close()
      if (conn != null)
        conn.close()
    }
  }

  def ipLong(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  def binarySearch(lines: Array[(String, String, String)], ip: Long): Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1.toLong) && (ip <= lines(middle)._2.toLong))
        return middle
      if (ip < lines(middle)._1.toLong)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }

  def main(args: Array[String]): Unit = {
 val conf = new SparkConf().setMaster("local").setAppName("IplocalSearch")
 // val conf =  new SparkConf().setAppName("IplocalSearch").setJars(Array("D:\\hadoop\\SparkPratice\\target\\SparkPratice-1.0-SNAPSHOT.jar")).setMaster("spark://hadoop:7077")//远程调用spark提交到集群
    val sc = new SparkContext(conf)
    val rulesAddArray = sc.textFile("E:\\计算IP地址归属地\\ip.txt").map(x => {
      val arr = x.split("\\|")
      (arr(2), arr(3), arr(6))
    }).collect()
    val ipRuleArray = sc.broadcast(rulesAddArray)
    val ipsRDD = sc.textFile("E:\\计算IP地址归属地\\20090121000132.394251.http.format").map(x => {
      val arr = x.split("\\|")
      arr(1)
    })
    val result = ipsRDD.map(ip => {
      val ipNum = ipLong(ip)
      val info = binarySearch(ipRuleArray.value, ipNum)
      (ip, ipRuleArray.value(info))
    }).map(x => (x._2._3, 1)).reduceByKey(_ + _)
    result.foreachPartition(dataMysql(_))
    println(result.collect().toBuffer)
    sc.stop()

  }


}
