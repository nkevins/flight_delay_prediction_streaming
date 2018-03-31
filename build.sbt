name := "flightstreaming"
version := "1.0"
scalaVersion := "2.11.11"


libraryDependencies += "joda-time" % "joda-time" % "2.9.9"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.5"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.6.5"
