package com.sparkTutorial.rdd.sumOfNumbers

import org.apache.spark._

object SumOfNumbersProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
       print the sum of those numbers to console.

       Each row of the input file contains 10 prime numbers separated by spaces.
     */

  
  val conf = new SparkConf().setAppName("SumOfNumbersProblem").setMaster("local[2]").set("spark.hadoop.validateOutputSpecs", "false")
  
  val sc = new SparkContext(conf)
  
  val primeFile = sc.textFile("in/prime_nums.text")
  primeFile.map( line => println(line))

  val primeNumbers = primeFile.flatMap(line => {val split = line.split("\\s+")
    split}).filter(line => ! (line == "")).map(line => line.trim.toInt)

  
  
  val sum = primeNumbers.reduce((x,y) => x + y)
  
  val count = primeNumbers.count()
  
  println(sum, " And the count is", count)

  } 
}
