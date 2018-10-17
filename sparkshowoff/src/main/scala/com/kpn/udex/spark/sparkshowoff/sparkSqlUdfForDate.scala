package com.kpn.udex.spark.sparkshowoff

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import java.io.File

object sparkSqlUdfForDate {
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
    import java.util.Date
    import java.text.SimpleDateFormat
    import java.util.Calendar

    val startOfTheWeek = udf((x: String) => {
		val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val timest = x.split(" ");
      val timestmp = timest(0).split("-");
		  val cal = Calendar.getInstance();
		  cal.set(Integer.parseInt(timestmp(0)), Integer.parseInt(timestmp(1)) - 1, Integer.parseInt(timestmp(2)))
		  cal.setFirstDayOfWeek(Calendar.MONDAY)
		  cal.set(Calendar.HOUR_OF_DAY, 0);
		  cal.set(Calendar.MINUTE, 0);
		  cal.set(Calendar.SECOND, 0);
		  cal.setMinimalDaysInFirstWeek(4);

		  if (cal.get(Calendar.DAY_OF_WEEK) == 1) {
			  cal.add(Calendar.DAY_OF_WEEK, cal.getFirstDayOfWeek() - 8);
		  } else {
			  cal.add(Calendar.DAY_OF_WEEK, cal.getFirstDayOfWeek() - (cal.get(Calendar.DAY_OF_WEEK)));
		  }
		  sdf.format(cal.getTime())
    })

    var df = Seq(
      ("1", "2018-10-05 00:30:00", "some_other_field_1"),
      ("2", "2018-10-09 00:07:00", "some_other_field_2")).toDF("key", "date", "blobData")

    df.printSchema
    df = df.withColumn("startOfTheWeek", startOfTheWeek($"date"))
      .select("key", "date", "startOfTheWeek")

    //.withColumn("jsonData", from_json($"jsonData", schema))
    df.printSchema
    df.show()
    println()
    spark.stop()
  }
}