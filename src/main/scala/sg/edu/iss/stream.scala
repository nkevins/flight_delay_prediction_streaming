package sg.edu.iss

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.joda.time.{DateTime, Days, DateTimeZone, Hours, Minutes}

object stream extends App {
  
  val sparkConf = new SparkConf().setAppName("Flight Streaming").setMaster("local[2]")
  val ssc = new StreamingContext(sparkConf, Seconds(5))
  
  // Load flight today schedule
  val spark = SparkSession.builder().master("local").appName("Data Processor").getOrCreate()
  import spark.implicits._
  val flightScheduleDf = spark.read.json("hdfs://localhost/user/cloudera/cag/" + DateTime.now.plusDays(1).toString("yyyyMMdd") + ".json").select(explode($"carriers").as("flights"))
  val flightNos = flightScheduleDf.select($"flights.flightNo").rdd.map(r => r(0)).collect()
  
  
  // Start streaming
  val lines = ssc.socketTextStream("localhost", 9008)
  
  val filteredLiveTraffic = lines.map(_.split(",")).filter(x => x.size >=7 && flightNos.contains(x(6)) && x(1) != "" && x(2) != "" && x(3) != "")
  
  filteredLiveTraffic.map(_.mkString(",")).print()
  
  ssc.start()
  ssc.awaitTermination()
}