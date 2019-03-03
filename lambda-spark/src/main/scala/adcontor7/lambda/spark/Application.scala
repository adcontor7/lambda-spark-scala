package adcontor7.lambda.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Application extends App {

  if (args.length < 2) {
    System.err.println(s"""
                          |Usage: Application <brokers> <topics>
                          |  <brokers> is a list of one or more Kafka brokers
                          |  <topics> is a list of one or more kafka topics to consume from
                          |
        """.stripMargin)
    System.exit(1)
  }

  val Array(brokers, topics) = args


  // Spark Configuration Object
  val conf = new SparkConf()
    //.setMaster("local[*]")
    .setAppName("Stream")

  val ssc = new StreamingContext(conf, Seconds(5))

  //BATH
  new Thread(new Batch(ssc.sparkContext)).start()


  //STREAM 1
  new Thread(new Stream(ssc, "RTView1", true, brokers, topics)).start()

  //STREAM 2
  new Thread(new Stream(ssc, "RTView2", false, brokers, topics)).start()

  Thread.sleep(5000) //WAIT UNTIL SARK CONTEXT IS CONFIGURATED
  ssc.start()
  ssc.awaitTermination()


}
