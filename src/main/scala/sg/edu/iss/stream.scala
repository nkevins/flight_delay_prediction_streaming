package sg.edu.iss

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.joda.time.{DateTime, Days, DateTimeZone, Hours, Minutes}
import org.joda.time.format.DateTimeFormat

object stream extends App {
  
  val sparkConf = new SparkConf().setAppName("Flight Streaming").setMaster("local[2]")
  val ssc = new StreamingContext(sparkConf, Seconds(5))
  
  // Load flight today schedule
  val spark = SparkSession.builder().master("local").appName("Data Processor").getOrCreate()
  import spark.implicits._
  val flightScheduleDf = spark.read.json("hdfs://localhost/user/cloudera/cag/" + DateTime.now.plusDays(0).toString("yyyyMMdd") + ".json").select(explode($"carriers").as("flights"))
  val flightNos = flightScheduleDf.select($"flights.flightNo").rdd.map(r => r(0)).collect()
  val flightInfo = flightScheduleDf.select($"flights.flightNo", $"flights.airportCode", $"flights.scheduledDatetime").rdd.map(r => (r(0), r(1), r(2))).collect()
  
  
  // Start streaming
  val lines = ssc.socketTextStream("localhost", 9008)
  
  val filteredLiveTraffic = lines.map(_.split(",")).filter(x => x.size >=7 && flightNos.contains(x(6)) && x(1) != "" && x(2) != "" && x(3) != "").map(processStreamData)
  
  filteredLiveTraffic.map(x => x.productIterator.mkString(",")).print()
  
  ssc.start()
  ssc.awaitTermination()
  
  def processStreamData(data: Array[String]) : (String, String, String, String, String, String, String, Double, Double, Boolean) = {
    val flightNo = data(6)
    val lat = data(1)
    val lon = data(2)
    val spd = data(3)
    val alt = data(4)
    val departure = flightInfo.filter(_._1 == flightNo)(0)._2.toString()
    val eta = flightInfo.filter(_._1 == flightNo)(0)._3.toString()
    val remainingDistance = calculateDistance(lat.toDouble, lon.toDouble, 1.35019, 103.994003)
    val remainingFlightTime = remainingDistance / spd.toDouble * 60
    val delayStatus = getDelayStatus(eta, remainingFlightTime)
    
    return (flightNo, lat, lon, spd, alt, departure, eta, remainingDistance, remainingFlightTime, delayStatus)
  }
  
  def calculateDistance(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    val theta = lon1 - lon2;
    val dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2)) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta));
    val dist2 = Math.acos(dist);
    val dist3 = rad2deg(dist2);
    val dist4 = dist3 * 60 * 1.1515;
    
    return dist4 * 0.8684
  }
  
  def deg2rad(deg: Double): Double = {
    return (deg * Math.PI / 180.0)
  }
  
  def rad2deg(rad: Double): Double = {
    return (rad * 180.0 / Math.PI)
  }
  
  def getDelayStatus(scheduledArrivalTime: String, remainingTime: Double): Boolean = {
    val formatter = DateTimeFormat.forPattern("yyyyMMdd HH:mm:ss")
    val schedArrTime = formatter.withZone(DateTimeZone.forID("Asia/Singapore")).parseDateTime(scheduledArrivalTime)
    val eta = DateTime.now(DateTimeZone.forID("Asia/Singapore")).plusMinutes(Math.round(remainingTime.toFloat))
    
    val actualDiff =  Minutes.minutesBetween(schedArrTime, eta).getMinutes
    if (actualDiff > 15) {
      true
    } else {
      false
    }
  }
}