package com.spark.sapient

import java.io.File

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import scala.collection.mutable.ListBuffer
import java.sql.Timestamp

/*
 
--------------- PROBLEM STATMENT -----------------
Given a time series data which is a clickstream of user activity is stored in hive, ask is to enrich the data with session id.
Session Definition:
•	Session expires after inactivity of 30 mins, because of inactivity no clickstream record will be generated. 
•	Session remains active for a total duration of 2 hours 
Steps:
•	Load Data in Hive Table.
•	Read the data from hive, use spark batch (Scala) to do the computation. 
•	Save the results in parquet with enriched data.
Note: Please do not use direct spark-sql.

timestamp	userid
2018-01-01T11:00:00Z	u1
2018-01-01T12:00:00Z	u1
2018-01-01T11:00:00Z	u2
2018-01-02T11:00:00Z	u2
2018-01-01T12:15:00Z	u1

                                                                               
                                                                  
In addition to the UPPER problem statement given assume below scenario as well and design hive table based on it:
•	Get Number of sessions generated in a day.
•	Total time spent by a user in a day 
•	Total time spent by a user over a month.
Here are the guidelines and instructions for the solution of above queries:
•	Design hive table
•	Write the script to create the table
•	Load data into table
•	Write all the queries in spark-sql
•	Think in the direction of using partitioning, bucketing, etc. 

 ---------------- SOLUTION IS BELOW DESINGED ----------------
 
•	Design hive table
•	Write the script to create the table
•	Load data into table
CREATE EXTERNAL TABLE IF NOT EXISTS USER_CLICKSTREAM_LOG (CLICK_TIMESTAMP TIMESTAMP, USER_ID STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION 'hdfs://ip-20-0-21-161:8020/user/edureka_431491/kram/dataset' TBLPROPERTIES("timestamp.formats"="yyyy-MM-dd'T'HH:mm:ss'Z'");


•	Write all the queries in spark-sql
SELECT USER_ID, CLICK_TIMESTAMP, (UNIX_TIMESTAMP(CLICK_TIMESTAMP) - UNIX_TIMESTAMP(LAG(CLICK_TIMESTAMP, 1) OVER(PARTITION BY USER_ID ORDER BY CLICK_TIMESTAMP)))/60 AS lOGGED_IN_TIME FROM USER_CLICKSTREAM_LOG;

SELECT USER_ID, UNIX_TIMESTAMP(CLICK_TIMESTAMP), RANK() OVER(PARTITION BY USER_ID ORDER BY CLICK_TIMESTAMP) AS RANK FROM USER_CLICKSTREAM_LOG;


---- CREATE A FINAL AGG TABLE ------
CREATE TABLE TABLE IF NOT EXISTS USER_CLICKSTREAM_SESSION(USER_ID String, CLICK_TIMESTAMP TIMESTAMP, SESSION_ID String, ActiveTimeInsSec Integer) PARTIOTIONED BY (USER_ID String) STORED AS PARQUET


------ •	Get Number of sessions generated in a day.
SELECT DAY(CLICK_TIMESTAMP) AS DAY_NAME, COUNT(SESSION_ID) AS NUMBER_OF_SESSION_IN_DAY FROM USER_CLICKSTREAM_SESSION GROUP BY SESSION_ID, DAY(CLICK_TIMESTAMP);


--- •	Total time spent by a user in a day 
SELECT USER_ID, DAY(CLICK_TIMESTAMP) AS DAY_NAME, SUM(ActiveTimeInsSec) AS TOT_TIME_SPENT FROM USER_CLICKSTREAM_SESSION GROUP BY USER_ID,DAY(CLICK_TIMES
TAMP);

--- •	Total time spent by a user over a month.
SELECT USER_ID, MONTH(CLICK_TIMESTAMP) AS DAY_NAME, SUM(ActiveTimeInsSec) AS TOT_TIME_SPENT FROM USER_CLICKSTREAM_SESSION GROUP BY USER_ID,MONTH(CLICK_TIMES
TAMP);

 */

object ClickstreamLogAnalyticsWindowFun {
 
  case class ResultRecord(User:String,ClickTime:Timestamp,Session:String,ActiveTimeInsSec:Long)
  
  def main(args: Array[String]): Unit = {

    // warehouseLocation points to the default location for managed databases and tables
    val warehouseLocation = new File("C:/hive_test/spark-warehouse").getAbsolutePath
    
    val spark = SparkSession
      .builder()
      .appName("Saipient Question 2")
      .master("local")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("javax.jdo.option.ConnectionURL","jdbc:derby:;databaseName=C:/hive_test/metastore_db;create=true")
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()
    
    import spark.sql
    import spark.implicits._

  /*
     * Here, Assuming clickstream data are ingested in HDFS at locaiton /user/hdfs/clickstream.
     * Creating EXTERNAL table on HDFS location.
     */
  val sqlStr = """CREATE EXTERNAL TABLE IF NOT EXISTS USER_CLICKSTREAM_LOG 
    (
    CLICK_TIMESTAMP TIMESTAMP, 
    USER_ID STRING
    ) 
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
    STORED AS TEXTFILE 
    LOCATION '/user/kram/dataset' 
    TBLPROPERTIES("timestamp.formats"="yyyy-MM-dd'T'HH:mm:ss'Z'")
    """

    sql(sqlStr)  

    // Unit Test Data -------------  start -- Hence commenting..........
    /*
    val clickstreamD = Seq(
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
      
      val windowSql = """SELECT 
        USER_ID, 
        CLICK_TIMESTAMP, 
        (UNIX_TIMESTAMP(CLICK_TIMESTAMP) - UNIX_TIMESTAMP(LAG(CLICK_TIMESTAMP, 1) OVER(PARTITION BY USER_ID ORDER BY CLICK_TIMESTAMP)))/60 AS lOGGED_IN_TIME 
        FROM USER_CLICKSTREAM_LOG """
      val df_test = spark.sql("windowSql")
      
      val distinctUser=df_test.select($"USER_ID").distinct.as[String].collect.toList
      
      val rankSql = """SELECT 
        USER_ID, 
        CLICK_TIMESTAMP, 
        RANK() OVER(PARTITION BY USER_ID ORDER BY CLICK_TIMESTAMP) AS RANK 
        FROM USER_CLICKSTREAM_LOG
        """
      val rankDF = sql(rankSql) 
     
      val rowList: ListBuffer[ResultRecord] = new ListBuffer()

      distinctUser.foreach{x =>{
          val tempDf= rankDF.filter($"USER_ID" === x)
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
              rowList += ResultRecord(r.getAs[String]("USER_ID"),r.getAs[Timestamp]("CLICK_TIMESTAMP"),r.getAs[String]("USER_ID")+"-"+session_index,dp)
            }
            else{
              session_index+=1
              cumulDiff = 0
              startBatch=true
              dp=0
              rowList += ResultRecord(r.getAs[String]("USER_ID"),r.getAs[Timestamp]("CLICK_TIMESTAMP"),r.getAs[String]("USER_ID")+"-"+session_index,dp)
            }
          } 
      }}

    // Saving final result in hive table using file format parquet. 

    sql("CREATE TABLE TABLE IF NOT EXISTS USER_CLICKSTREAM_SESSION(USER_ID String, CLICK_TIMESTAMP TIMESTAMP, SESSION_ID String, ActiveTimeInsSec Integer) PARTIOTIONED BY (USER_ID String) STORED AS PARQUET")
   
    val finalDF = spark.sparkContext
    .parallelize(rowList.toList)
    .toDF("USER_ID","CLICK_TIMESTAMP","SESSION_ID")
    .write.mode(SaveMode.Append)
    .partitionBy("USER_ID")
    .saveAsTable("USER_CLICKSTREAM_SESSION")
    
    // Get Number of sessions generated in a day.
    val noOSessionsInDayDF = sql("SELECT DAY(CLICK_TIMESTAMP) AS DAY_NAME, COUNT(SESSION_ID) AS NUMBER_OF_SESSION_IN_DAY FROM USER_CLICKSTREAM_SESSION GROUP BY SESSION_ID, DAY(CLICK_TIMESTAMP)")
    
    // Total time spent by a user in a day 
    val totTmeSpentUserInDayDF = sql("SELECT USER_ID, DAY(CLICK_TIMESTAMP) AS DAY_NAME, SUM(ActiveTimeInsSec) AS TOT_TIME_SPENT FROM USER_CLICKSTREAM_SESSION GROUP BY USER_ID,DAY(CLICK_TIMESTAMP)")

    // Total time spent by a user in a day 
    val totTmeSpentUserInMonthDF = sql("SELECT USER_ID, MONTH(CLICK_TIMESTAMP) AS DAY_NAME, SUM(ActiveTimeInsSec) AS TOT_TIME_SPENT FROM USER_CLICKSTREAM_SESSION GROUP BY USER_ID,MONTH(CLICK_TIMESTAMP)")
    
  }
}