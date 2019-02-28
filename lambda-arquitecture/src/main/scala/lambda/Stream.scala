package lambda

import java.time.Instant
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import redis.clients.jedis.Jedis
class Stream(conf: SparkConf, view: String) extends Runnable{

  override def run(): Unit = {

    val hdfsFolder = "hdfs://localhost/new/"

    val topics = "example"
    val brokers = "localhost:9092"
    val groupId = "lambda_stream"
    val topicsSet: Set[String] = topics.split(",").toSet
    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val streamingContext = new StreamingContext(conf, Seconds(10))

    val kstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
    )




    // Redis client setup
    var redisClient: Jedis = null

    val parseRecord: ConsumerRecord[String, String] => (String, Int) = (r:ConsumerRecord[String, String]) => {
      try {
        (view + "::" + r.key, r.value.toInt)
      } catch {
        case e: Exception => (r.key, 1)
      }
    }


    kstream
      .filter(record => record.key.nonEmpty)
      .map(parseRecord)
      .foreachRDD(rdd => {
        if (rdd.count() > 0) {

          rdd.saveAsTextFile(hdfsFolder + Instant.now().getEpochSecond)
          rdd.foreachPartition(partitionOfRecords => {
            redisClient = new Jedis("localhost", 6379) //TODO-TD USE POOL
            partitionOfRecords.foreach(record => redisClient.incrBy(record._1, record._2))
            redisClient.close()
          })

        }

      })



    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
