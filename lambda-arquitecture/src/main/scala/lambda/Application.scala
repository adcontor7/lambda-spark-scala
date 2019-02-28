package lambda

import java.time.Instant

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object Application extends App {

  // You may or may not want to enable some additional DEBUG logging
  Logger.getLogger("org.apache.spark.streaming.dstream.DStream").setLevel(Level.DEBUG)
  Logger.getLogger("org.apache.spark.streaming.dstream.WindowedDStream").setLevel(Level.DEBUG)
  Logger.getLogger("org.apache.spark.streaming.DStreamGraph").setLevel(Level.DEBUG)
  Logger.getLogger("org.apache.spark.streaming.scheduler.JobGenerator").setLevel(Level.DEBUG)

  /*if (args.length < 2) {
    System.err.println(s"""
                          |Usage: Stream <brokers> <topics>
                          |  <brokers> is a list of one or more Kafka brokers
                          |  <topics> is a list of one or more kafka topics to consume from
                          |
        """.stripMargin)
    System.exit(1)
  }

  val Array(brokers, topics) = args
  */



  // Spark Configuration Object
  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Stream")

  //val stream1: Unit = new Thread(new Stream(conf, "RTView1")).start()
  //val stream2: Unit = new Thread(new Stream(conf, "RTView2")).start()

  val batch: Unit = new Thread(new Batch(conf, "BView")).start()




}
