package com.kpn.udex.spark.kafkastreamlearining

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.SQLContext
// Import SQL functions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
// Define udf
import org.apache.spark.sql.functions.udf
import org.apache.spark.rdd.RDD
import org.apache.kafka.common.serialization.ByteArrayDeserializer
    
object processKafkaDirectStream {
    def main(args: Array[String]) {
    val brokers = Array("localhost:9092")
    val topics = Array("test")
    
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("processKafkaDirectStream")

    val ssc = new StreamingContext(sparkConf, Seconds(20))
 // Create direct kafka stream with brokers and topics
    val topicsSet = topics.toSet
    val kafkaParams = Map[String, Object](
      "metadata.broker.list" -> brokers,
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[ByteArrayDeserializer],
      "group.id" -> "myStreamTestConsumerGroup",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean))
      
    val messages = KafkaUtils.createDirectStream[String, Array[Byte]](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, Array[Byte]](topicsSet, kafkaParams))

      val USER_SCHEMA = "{\"fields\": [{ \"name\": \"firstname\", \"type\": \"string\" },{ \"name\": \"secondname\", \"type\": \"string\" },{ \"name\": \"age\", \"type\": \"int\" } ], \"name\": \"namerecord\",\"type\": \"record\"}";

      import org.apache.avro.Schema;
      import org.apache.avro.generic.GenericData;
      import org.apache.avro.generic.GenericRecord;
      import com.twitter.bijection.Injection;
      import com.twitter.bijection.avro.GenericAvroCodecs;
      object InjectionForMe {
        val parser = new Schema.Parser()
        val schema = parser.parse(USER_SCHEMA)
        val injection: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)
      }
      
    messages.foreachRDD(rdd => {      
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      val df = rdd.map ( avroRecord => InjectionForMe.injection.invert(avroRecord.value).get)
      .map(record => User(record.get("firstname").toString, record.get("secondname").toString, record.get("age").toString.toInt) 
        ).toDF()
                     
      df.show()
    })
    ssc.start()
    ssc.awaitTermination()
}
    
}
/** Case class for converting RDD to DataFrame */
case class User(firstName: String, secondName: String, age: Int)