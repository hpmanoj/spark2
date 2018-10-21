package com.sparkTutorial.pairRdd.sort

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.{ SparkConf, SparkContext }

object SortedWordCountProblem {

  /* Create a Spark program to read the an article from in/word_count.text,
       output the number of occurrence of each word in descending order.

       Sample output:

       apple : 200
       shoes : 193
       bag : 176
       ...
     */
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("avgHousePrice").setMaster("local[3]")
    val sc = new SparkContext(conf)
    
    val lines = sc.textFile("in/word_count.text")
    val words = lines.flatMap( line => line.split(" "))
    
    val pairOfWords = words.map(line => (line, 1))
    
    val counts = pairOfWords.reduceByKey((x,y) => x +y)
    
    //val sorted = counts.sortByKey(true)
    
    //for((word , count) <- sorted.collect ) println (word + " : " + count)
    
    val flipcount = counts.map( line => (line._2, line._1))
    
    val sorted = flipcount.sortByKey(false)
    
    for((count , word) <- sorted.collect ) println ( word + " : " + count)
  }

}

