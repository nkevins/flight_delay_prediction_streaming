package sg.edu.iss

import org.apache.spark.SparkConf
import org.apache.spark.streaming._

object stream extends App {
  
  val sparkConf = new SparkConf().setAppName("Flight Streaming").setMaster("local[2]")
  val ssc = new StreamingContext(sparkConf, Seconds(5))
  
  val lines = ssc.socketTextStream("localhost", 9008)
  
  val info = lines.map(_.split(",")).filter(x => x.size >=7 && x(6).startsWith("SQ")).map(_.mkString(","))
  
  info.print()
  
  ssc.start()
  ssc.awaitTermination()
  
}