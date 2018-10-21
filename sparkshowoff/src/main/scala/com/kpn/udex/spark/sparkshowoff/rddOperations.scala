package com.kpn.udex.spark.sparkshowoff

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object rddOperations {
  def main(args: Array[String]) {
     val sparkConf = new SparkConf().setMaster("local[2]").setAppName("rddOperations")
     val sc = new SparkContext(sparkConf)
     val data = Array(1, 2, 3, 4, 5)
     val distData = sc.parallelize(data)

     println(distData.collect())
     
     val textFile = sc.textFile("src//main//resources//log4j.properties")
     
     val count = textFile.count()
     val firstline = textFile.first()
     println(count)
     println(firstline)
     
     val linesWithSpark = textFile.filter(line => line.contains("Spark"))
     
     println(linesWithSpark)
     sc.stop()
  }
}