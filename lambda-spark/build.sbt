
name := "Lambda-spark-example"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.4.0" % "provided",
  "org.apache.spark" % "spark-streaming_2.11" % "2.4.0"  % "provided",
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.4.0",
  "redis.clients" % "jedis" % "2.9.0"
)

fork in run := true
javaOptions in run ++= Seq(
  "-Dlog4j.debug=true",
  "-Dlog4j.configuration=log4j.properties")
outputStrategy := Some(StdoutOutput)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case PathList(xs @ _*) if xs.last == "UnusedStubClass.class" => MergeStrategy.first
  case "unwanted.txt"                                => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
