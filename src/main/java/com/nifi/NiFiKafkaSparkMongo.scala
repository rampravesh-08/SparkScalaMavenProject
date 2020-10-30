package com.nifi

import org.apache.spark.sql.SparkSession
import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


/*
 * Set VM Argument In Run Configuration - > Arguments (Tab) - VM Arguments = -Xmx512m
 */
object NiFiKafkaSparkMongo {
  
  case class CLIParams(mongoUri: String = "")
  
  case class Address(building: String, coord: Array[Double], street: String, zipcode: String)
  
  case class Restaurant(address: Address, borough: String, cuisine: String, name: String)

  def main(args: Array[String]): Unit = {
    
     val spark = SparkSession.builder()
        .master("local")
        .appName("MongoSparkConnectorIntro")
        .config("spark.driver.allowMultipleContexts","true")
        .config("spark.mongodb.input.uri", "mongodb://localhost:27017/kram.nifi_kafka_spark_mongo")
        .config("spark.mongodb.output.uri", "mongodb://localhost:27017/kram.nifi_kafka_spark_mongo")
        .getOrCreate()
        
        
        
     import spark.implicits._
     
     val conf = new SparkConf().setMaster("local")
       .setAppName("MongoSparkConnectorIntro")
       .set("spark.driver.allowMultipleContexts","true")
     
     val ssc = new StreamingContext(conf,Seconds(2))
     
     val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
     )
    
      val topics = Array("test")
      
      val stream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )
      
      //stream.print()
      val streamData = stream.map(record => record.value)
      streamData.foreachRDD(rdd =>{
        if(!rdd.isEmpty()){
          val dataJsonDF = spark.read.json(rdd)
          MongoSpark.save(dataJsonDF.write.mode(SaveMode.Append))
        }
      })
   
     ssc.start()
     ssc.awaitTermination()
     
  }
}