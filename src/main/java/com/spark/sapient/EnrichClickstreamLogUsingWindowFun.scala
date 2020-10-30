package com.spark.sapient

import java.io.File

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import scala.collection.mutable.ListBuffer
import java.sql.Timestamp

object EnrichClickstreamLogUsingWindowFun {
  
  case class ResultRecord(User:String,ClickTime:Timestamp,Session:String)

  def main(args: Array[String]): Unit = {
    
    // warehouseLocation points to the default location for managed databases and tables
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    
    val spark = SparkSession
      .builder()
      .appName("Saipient Question 2")
      .master("local")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    
    import spark.sql
    import spark.implicits._

    /*
     * Here, Assuming clickstream data are ingested in HDFS at locaiton /user/hdfs/clickstream.
     * Creating EXTERNAL table on HDFS location.
     */
    sql("CREATE EXTERNAL TABLE IF NOT EXISTS USER_CLICKSTREAM_LOG (CLICK_TIMESTAMP TIMESTAMP, USER_ID STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS ORC LOCATION /user/hdfs/clickstream")

    val clickstream = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss'Z'")
    .option("format", "orc")
    .load()
    
    
    // Unit Test Data -------------  start -- Hence commenting..........
    /*
    val clickstream = Seq(
       2018-01-01T11:00:00Z", "u1"),
       2018-01-01T12:10:00Z", "u1"),
       2018-01-01T13:00:00Z", "u1"),
       2018-01-01T13:50:00Z", "u1"),
       2018-01-01T14:40:00Z", "u1"),
       2018-01-01T15:30:00Z", "u1"),
       2018-01-01T16:20:00Z", "u1"),
       2018-01-01T16:50:00Z", "u1"),
       2018-01-01T11:00:00Z", "u2"),
       2018-01-02T11:00:00Z", "u2")
      )
      .toDF("CLICK_TIME", "USER_ID")
      .withColumn("CLICK_TIMESTAMP",unix_timestamp($"CLICK_TIME", "yyyy-MM-dd'T'HH:mm:ss'Z'").cast(TimestampType))
      .drop("CLICK_TIME")
      */
     // Unit Test Data -------------  end
  
      val partitionWindow = Window.partitionBy($"USER_ID").orderBy($"CLICK_TIMESTAMP".asc)

      val lagTest = lag($"CLICK_TIMESTAMP", 1, "0000-00-00 00:00:00").over(partitionWindow)
      val df_test=  clickstream.select($"*", ((unix_timestamp($"CLICK_TIMESTAMP")-unix_timestamp(lagTest))/60D cast "int") as "DIFF_WITH_PREV")

      val distinctUser=df_test.select($"USER_ID").distinct.as[String].collect.toList

      val rankTest = rank().over(partitionWindow)
      val ddf = df_test.select($"*", rankTest as "rank")
      
      // distinctUser.foreach(println)
      
      val rowList: ListBuffer[ResultRecord] = new ListBuffer()

      distinctUser.foreach{x =>{
          val tempDf= ddf.filter($"USER_ID" === x)
          var cumulDiff:Int=0
          var session_index=1
          var startBatch=true
          var dp=0
          val len = tempDf.count.toInt
          for(i <- 1 until len+1){
            val r = tempDf.filter($"rank" === i).head()
            dp = r.getAs[Int]("DIFF_WITH_PREV")
            cumulDiff += dp
            if(dp <= 30 && cumulDiff <= 120){
              startBatch=false
              rowList += ResultRecord(r.getAs[String]("USER_ID"),r.getAs[Timestamp]("CLICK_TIMESTAMP"),r.getAs[String]("USER_ID")+"-"+session_index)
            }
            else{
              session_index+=1
              cumulDiff = 0
              startBatch=true
              dp=0
              rowList += ResultRecord(r.getAs[String]("USER_ID"),r.getAs[Timestamp]("CLICK_TIMESTAMP"),r.getAs[String]("USER_ID")+"-"+session_index)
            }
          } 
      }}

 
    // Saving final result in parquet file.
    val finalDF = spark.sparkContext
    .parallelize(rowList.toList)
    .toDF("USER_ID","CLICK_TIMESTAMP","SESSION_ID")
    .write.parquet("user_session.parquet")
      
  }
}