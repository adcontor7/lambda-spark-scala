package adcontor7.lambda.spark

import java.net.URI
import java.util

import adcontor7.lambda.{HdfsClient, RedisClient}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileAlreadyExistsException, FileSystem, FileUtil, Path}
import org.apache.hadoop.mapred.InvalidInputException
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.{Jedis, ScanParams}

import scala.collection.JavaConverters._


class Batch(sc: SparkContext, brokers: String, redisClient: RedisClient, hdfs: HdfsClient) extends Runnable{

  override def run(): Unit = {

    val props = new util.HashMap[String, Object]()

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    val kProducer = new KafkaProducer[String, String](props)
    val endMessage = new ProducerRecord[String, String]("batch-ended", "OK")

    while (true) {
      println("Start Batch processing")
      hdfs.mvNewData()

      //Clear Batch view
      val redis = redisClient.getInstance
      redis.scan(0, new ScanParams().`match`("BView::*"))
        .getResult.asScala.foreach(k => redis.del(k))
      redis.close()


      process(sc)
      //Emulate Batch Processing time is up to 5s
      Thread.sleep(5000)

      kProducer.send(endMessage)
      //Sleep 5 seconds until RTViews switch
      Thread.sleep(5000)
    }

  }

  def process(sc: SparkContext): Unit = {


    val splitToTuple: String => (String, Int) = (s: String) => {
      val toRemove = "()".toSet
      val clean = s.filterNot(toRemove)
      val Array(k,v) = clean.split(",")
      ("BView::".concat(k), v.toInt)
    }

    var redis: Jedis = null
    try {
      sc.textFile(hdfs.rawDataSet)
        .map(splitToTuple)
        .reduceByKey(_ + _) //Sum same key values
        .foreachPartition(partitionOfRecords => {
          //NOTE : EACH PARTITION ONE CONNECTION (more better way is to use connection pools)
          redis = redisClient.getInstance
          //Batch size of 1000 is used since some databases cant use batch size more than 1000 for ex : Azure sql
          partitionOfRecords.grouped(1000).foreach(group => {
            group.foreach(record => {
              println(record)
              redis.set(record._1.toString, record._2.toString)
            })
          })
          redis.close()
        })
    } catch {
      case e: InvalidInputException => System.out.println("RAW is empty")
    }
  }
}
