//import AssemblyKeys._

name := "Lambda-spark-example"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.4.0",
  "org.apache.spark" % "spark-streaming_2.11" % "2.4.0",
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.4.0",
  "redis.clients" % "jedis" % "2.9.0"
)
