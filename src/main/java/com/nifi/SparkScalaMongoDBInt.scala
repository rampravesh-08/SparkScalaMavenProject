package com.nifi

import org.apache.spark.sql.SparkSession
import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SaveMode

/*
 * Set VM Argument In Run Configuration - > Arguments (Tab) - VM Arguments = -Xmx512m
 */
object SparkScalaMongoDBInt {
  
  case class CLIParams(mongoUri: String = "")
  
  case class Address(building: String, coord: Array[Double], street: String, zipcode: String)
  
  case class Restaurant(address: Address, borough: String, cuisine: String, name: String)

  def main(args: Array[String]): Unit = {
    
     val spark = SparkSession.builder()
        .master("local")
        .appName("MongoSparkConnectorIntro")
        .config("spark.mongodb.input.uri", "mongodb://localhost:27017/kram.restaurants")
        .config("spark.mongodb.output.uri", "mongodb://localhost:27017/kram.restaurants")
        .getOrCreate()
      
     val data = 
       Seq(
           Restaurant(Address("1480", Array(-73.9557413, 40.7720266), "2 Avenue", "10075"), "Manhattan", "Italian", "Vella"), 
           Restaurant(Address("1007", Array(-73.856077, 40.848447), "Morris Park Ave", "10462"), "Bronx", "Bakery", "Morris Park Bake Shop")
       )
  
     import spark.implicits._
     val dfRestaurants = spark.sparkContext.parallelize(data).toDF()
     
     dfRestaurants.show()
     MongoSpark.save(dfRestaurants.write.mode(SaveMode.Overwrite))
     
  }
}