package com.sparkTutorial.rdd.nasaApacheWebLogs

import org.apache.spark._
import org.apache.spark.sql.catalyst.expressions.Contains
import org.apache.spark.sql.catalyst.expressions.StartsWith
import com.sparkTutorial.commons.Utils

object SameHostsProblem {

  def main(args: Array[String]) {

    /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
       "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
       Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
       Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

       Example output:
       vagrant.vf.mmc.com
       www-a1.proxy.aol.com
       .....

       Keep in mind, that the original log files contains the following header lines.
       host	logname	time	method	url	response	bytes

       Make sure the head lines are removed in the resulting RDD.
     */
    val conf = new SparkConf().setAppName("SameHostsProblem").setMaster("local[2]").set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext(conf)
    
    val nasa_19950701 = sc.textFile("in/nasa_19950701.tsv").filter(line => isHeader(line))
    .map(line => {
      val split = line.split('\t')
      split(0)
    })
    val nasa_19950801 = sc.textFile("in/nasa_19950801.tsv").filter(line => isHeader(line))
    .map(line => {
      val split = line.split('\t')
      split(0)
    })
    
    val intersect = nasa_19950701.intersection(nasa_19950801)
    intersect.coalesce(1).saveAsTextFile("out/nasa_logs_same_hosts.csv")
    
  }
      def isHeader(line: String): Boolean = !(line.startsWith("host") && line.contains("bytes"))

}
