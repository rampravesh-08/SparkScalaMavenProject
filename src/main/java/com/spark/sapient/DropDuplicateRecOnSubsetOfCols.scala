package com.spark.sapient

import java.io.File

import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
QUESTION -1 (General Coding)
From the sample data given below, remove duplicates on the combination of Name and Age and print results. 
•	Please do not use high level API/Framework like pandas /spark-sql etc. 
•	Solve this problem by using simple data structures given in a programming language. 
•	Please try to optimize the solution for efficiency in terms of space and time.

Given Dataset:
Name	Age	Location 
Rajesh 	21	London
Suresh	28	California
Sam	26	Delhi
Rajesh 	21	Gurgaon
Manish	29	Bengaluru

 
 */
object DropDuplicateRecOnSubsetOfCols {
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession
      .builder()
      .appName("Saipient Test 1")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()
    
    import spark.implicits._
    
    // Assuming Given Dataset in file dataset1.csv
    val rdd = spark.sparkContext.textFile("dataset1.csv")
    /*
      Name,Age,Location
      Rajesh,21,London
      Suresh,28,California
      Sam,26,Delhi
      Rajesh,21,Gurgaon
      Manish,21,Bangaluru
     */
    
    // Removing the 1st record
    val headerRow = rdd.first()
    val dataRDD = rdd.filter(rec => rec != headerRow)
    
    /*
     * 1. Mapped record Key-Value which is Key is (Name and Age) and value is Record line.
     * 2. Perform the reduce operation on Key and kept unique entry against each KEY (Name and Age)
     */
    val distinctRecordsResult = dataRDD.map(rec =>{
      val splitArray = rec.split(",")
      ( (splitArray(0),splitArray(1)) , rec)
    })
    .reduceByKey( (redc1, redc2) => redc1)
    .map({ case (k,v)  => v })
    .collect()
    
    println("------- Distinct Records on Name And Age ------")
    distinctRecordsResult.foreach(println)
   
  }
}