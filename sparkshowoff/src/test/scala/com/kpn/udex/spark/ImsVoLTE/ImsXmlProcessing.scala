package com.kpn.udex.spark.ImsVoLTE

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
// Import SQL functions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
// Define udf
import org.apache.spark.sql.functions.udf
import scala.collection.mutable._
import org.apache.spark.sql.types._

import org.apache.spark.sql._

object ImsXmlProcessing {

  def main(args: Array[String]) {
    val conf = new SparkConf().set("spark.driver.memory", "471859200").setAppName("IMSXmlProcessing").setMaster("local[1]");
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    //  Specify the tag that represents a record for parsing, and files that need to be parsed (The URL can be for HDFS, to access files in HDFS)
    val dfMetaData = sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rootTag", "measCollecFile")
      .option("rowTag", "fileHeader")
      .load("src/test/resources/A20171102.1440+0000-1445+0000_SubNetwork=as3smManagedElement=ces-1")
    //      .load("file://C://NDA_UDEX//PoC//UdexSparkProcessing//src//main//resources//A20171102.0805+0000-0810+0000_SubNetwork=ah1bc,ManagedElement=ces-1")

    dfMetaData.printSchema()
    val selectMetaData = dfMetaData.select(
      $"fileSender._elementType".as("elementType"),
      $"fileSender._localDn".as("localDn"),
      $"measCollec._beginTime".as("beginTime"))
      .select("elementType", "localDn", "beginTime")

    val dfRowData = sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rootTag", "measCollecFile")
      .option("rowTag", "measData")
      .load("src/test/resources/A20171102.1440+0000-1445+0000_SubNetwork=as3smManagedElement=ces-1")

    //  Select fields in a record, and this is the first step of parsing the XML to records
    val dfRowMeasInfo = dfRowData.select(
      $"managedElement._localDn".as("localDn"),
      $"managedElement._swVersion".as("swVersion"),
      $"managedElement._userLabel".as("userLabel"),
      explode($"measInfo").as("measInfo"))

       
    // MeasInfo is processed further to get values needed for the output
    val dfRowMeasInfoExploded = dfRowMeasInfo.select(
      $"localDn",
      $"swVersion",
      $"userLabel",
      $"measInfo.granPeriod._endTime".as("endTime"),
      $"measInfo.measType".as("Keys"),
      $"measInfo",
      explode($"measInfo.measValue").as("Values"))
      .select("localDn", "swVersion", "userLabel", "endTime", "Values._measObjLdn", "Keys", "Values.r")

    val dfRowMeasInfoKeyValue = dfRowMeasInfoExploded.select(
      $"localDn",
      $"swVersion",
      $"userLabel",
      $"endTime",
      $"_measObjLdn",
      $"Keys._VALUE".as("key"),
      $"r._VALUE".as("value"))

    import org.apache.spark.sql.functions.{ udf, explode }
    val zip = udf((xs: Seq[String], ys: Seq[Double]) => xs.zip(ys))

    val dfRowMeasInfoZipKeyValue = dfRowMeasInfoKeyValue.withColumn("vars", explode(zip($"Key", $"value")))
      .select($"localDn", $"swVersion", $"userLabel", $"endTime", $"_measObjLdn",
        regexp_replace($"vars._1", "\\.+","_").alias("Key"), $"vars._2".alias("value"))

    val dfRowMeasInfoJson = dfRowMeasInfoZipKeyValue
      .withColumn("NameValueConcat", concat_ws(":", format_string("\"%s\"", $"key"), format_string("\"%s\"", $"value")))
      .select(
        $"localDn",
        $"swVersion",
        $"userLabel",
        $"endTime",
        $"_measObjLdn",
        $"NameValueConcat".as("jsonNameValue"))
      .groupBy($"localDn", $"swVersion", $"userLabel", $"endTime", $"_measObjLdn")
      .agg(collect_list($"jsonNameValue").as("jsonNameValue"))
      .withColumn("jsonNameValue", format_string("{%s}", concat_ws(",", $"jsonNameValue")))
      .select(
        $"localDn".as("localDnKey"),
        $"swVersion",
        $"userLabel",
        $"endTime",
        format_string("%s%s", $"localDn", $"_measObjLdn").as("distinguishedName"),
        $"jsonNameValue".as("JsonCounters"))

    val joined = dfRowMeasInfoJson.join(selectMetaData, $"localDn" === $"localDnKey")
      .select(unix_timestamp($"beginTime", "yyyy-MM-dd'T'HH:mm:ssXXX").as("Timestamp"), $"DistinguishedName", $"JsonCounters")



      joined.cache()
 
    val FileSystem = joined.withColumn("DistinguishedNameFilter", regexp_extract($"DistinguishedName", "([^ ,]+)=[^,]+$", 1))
      .filter($"DistinguishedNameFilter".like("FileSystem"))
      .select($"Timestamp".as("period"), $"DistinguishedName".as("distinguished_name"),  
          get_json_object($"JsonCounters", "$.VS_fileSysUsage").alias("VS_fileSysUsage"))
      .select($"period", $"distinguished_name",  when($"VS_fileSysUsage".isNull, 0.0).otherwise($"VS_fileSysUsage").as("VS_fileSysUsage"))
     

    // Here goes the code for inserting records to ServiceGaurd MySQL
    FileSystem.show(10, false)
   
   
    FileSystem.createOrReplaceTempView("FileSystem")
   //joined.printSchema()
   joined.createOrReplaceTempView("pm_all_elements")
   
   val FileSystem_SQL = sqlContext.sql("""
     SELECT Timestamp, 
            DistinguishedName, 
            get_json_object(JsonCounters, "$.VS_fileSysUsage") as VS_fileSysUsage 
     FROM pm_all_elements
     WHERE 
            regexp_extract(DistinguishedName, "([^ ,]+)=[^,]+$", 1) = "FileSystem"
     """)
   
    FileSystem_SQL.show(10, false)


    import java.util.Properties

    // Option 1: Build the parameters into a JDBC url to pass into the DataFrame APIs
    //Eventually these will come from job.properties once integrated with oozie workflow
    val jdbcUsername = "mr00122459"
    val jdbcPassword = "password"
    val jdbcHostname = "localhost"
    val jdbcPort = 3306
    val jdbcDatabase = "servicegaurd"
    
    val connectionProperties = new Properties()
    connectionProperties.put("user", "mr00122459")
    connectionProperties.put("password", "password")
    connectionProperties.setProperty("driver", "com.mysql.jdbc.Driver")

    // JDBC URL alteration need for SSL ,and load balancing
    val jdbc_url = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"
    
    //val temp_table = sqlContext.read.jdbc(jdbc_url, "servicegaurd.test", connectionProperties)
    sqlContext.table("FileSystem")
    .write.mode(SaveMode.Append)
    .jdbc(jdbc_url, "FileSystem_5m", connectionProperties)

        
    val FinalForHiveWrite = joined.select($"Timestamp", $"DistinguishedName", regexp_replace($"JsonCounters", "\\_+","."))

    FinalForHiveWrite.show(10, false)
    sc.stop()



    /* 
        val schema = StructType(Seq(
      StructField("k", StringType, true), StructField("v", StringType, true)))
      
       val FileSystem = joined.withColumn("DistinguishedNameFilter", regexp_extract($"DistinguishedName", "([^ ,]+)=[^,]+$", 1))
       .withColumn("json", from_json($"JsonCounters", schema))
       .filter($"DistinguishedNameFilter".like("FileSystem"))
       .select($"Timestamp".as("period"), $"DistinguishedName".as("distinguished_name"), $"JsonCounters", $"json")

     
     * 
     * 
     *   val selectedData11 = selectedData10.select($"localDn",
      $"swVersion",
      $"userLabel",
      $"endTime",
      $"_measObjLdn",
      concat_ws(":", $"value"),
      to_json(struct($"key", $"value")).as("jsonNameValue"))
      .groupBy($"localDn",$"swVersion", $"userLabel", $"endTime", $"_measObjLdn")
      .agg(collect_list($"jsonNameValue").as("jsonNameValue"))
      .select($"localDn",
      $"swVersion",
      $"userLabel",
      $"endTime",
      $"_measObjLdn",
      $"jsonNameValue")


   val selectedData7 = selectedData6.select($"localDn",
      $"swVersion",
      $"userLabel",
      $"endTime",
      $"_measObjLdn",
      explode(zip($"Keys._VALUE",$"r._VALUE" )).as("nameValuePair"))
      .groupBy($"localDn",$"swVersion", $"userLabel", $"endTime", $"_measObjLdn")
      .agg(collect_list($"nameValuePair").as("nameValuePair"))
      .select($"localDn",
      $"swVersion",
      $"userLabel",
      $"endTime",
      $"_measObjLdn",
      to_json(struct($"nameValuePair")))

    val selectedData8 = selectedData6.select($"localDn",
      $"swVersion",
      $"userLabel",
      $"endTime",
      $"_measObjLdn",
      explode(zip($"Keys._VALUE",$"r._VALUE" )).as("nameValuePair"))
      .groupBy($"localDn",$"swVersion", $"userLabel", $"endTime", $"_measObjLdn")
      .agg(collect_list($"nameValuePair").as("nameValuePair"))
      .select($"localDn",
      $"swVersion",
      $"userLabel",
      $"endTime",
      $"_measObjLdn",
      $"nameValuePair")


    selectedData8.show(10, false)
     return
  */

  }
}