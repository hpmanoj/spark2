package com.kpn.udex.spark.sparkshowoff

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import java.io.File

object jsonParsingInDf {
  def main(args: Array[String]) {

    // warehouseLocation points to the default location for managed databases and tables
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath    
    val spark = SparkSession
      .builder()
      .appName("rddOperations")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      //.enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
      
     val schema = StructType(Seq(
        StructField("k", StringType, true), StructField("v", DoubleType, true)
      ))

     var df = Seq(
        ("1", """{"k": "foo", "v": 1.0}""", "some_other_field_1"),
        ("2", """{"k": "bar", "v": 3.0}""", "some_other_field_2")
    ).toDF("key", "jsonData", "blobData")
    
    df = df.withColumn("k", get_json_object($"jsonData", "$.k"))
    .withColumn("v", get_json_object($"jsonData", "$.v"))
    .select("key", "blobData", "k", "v")
    
    //.withColumn("jsonData", from_json($"jsonData", schema))

    df.show()
     println()
     spark.stop()
  }
}