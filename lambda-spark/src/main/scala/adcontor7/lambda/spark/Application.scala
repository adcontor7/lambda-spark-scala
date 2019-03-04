package adcontor7.lambda.spark

import adcontor7.lambda.{HdfsClient, RedisClient}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.{Level, Logger}

object Application extends App {

  // Spark Configuration Object
  val conf = new SparkConf()
    .setAppName("lambda-spark")

  /*val brokers = "localhost:9092"
  val dataTopics = "wordcount"
  val redisHost = "localhost:6379"
  val hdfsHost = "hdfs://localhost/"
  conf.setMaster("local[*]")*/

  if (args.length != 4) {
    System.err.println("Starting in local mode")
    System.err.println(s"""
                          | Application <brokers> <topics>
                          |  <brokers> is a list of one or more Kafka brokers
                          |  <dataTopics> data topics
                          |  <redisHost> redis Host Ej. redis.local:6379
                          |  <hdfsHost> Hdfs Host Ej. hdfs://localhost/
                          |
        """.stripMargin)
    System.exit(1)
  }
  val Array(brokers, dataTopics, redisHost, hdfsHost) = args



  val ssc = new StreamingContext(conf, Seconds(5))

  val rootLogger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)


  val redisClient = new RedisClient(redisHost)
  val hdfsClient = new HdfsClient(hdfsHost)

  //BATH
  new Thread(new Batch(ssc.sparkContext, brokers, redisClient, hdfsClient)).start()


  //STREAM 1
  new Thread(new Stream(ssc, "RTView1", true, brokers, dataTopics, redisClient, hdfsClient)).start()

  //STREAM 2
  new Thread(new Stream(ssc, "RTView2", false, brokers, dataTopics, redisClient, hdfsClient)).start()

  Thread.sleep(5000) //WAIT UNTIL SARK CONTEXT IS CONFIGURATED
  ssc.start()
  ssc.awaitTermination()


}
