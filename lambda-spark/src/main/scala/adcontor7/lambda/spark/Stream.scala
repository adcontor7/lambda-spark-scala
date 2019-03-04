package adcontor7.lambda.spark

import java.time.Instant

import adcontor7.lambda.{HdfsClient, RedisClient}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Accumulator, SparkConf}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import redis.clients.jedis.{Jedis, ScanParams, ScanResult}

import scala.collection.JavaConverters._

class Stream(
              ssc: StreamingContext,
              view: String, active: Boolean,
              brokers: String, topics: String,
              redisClient: RedisClient, hdfs: HdfsClient
            ) extends Runnable{

  var _viewBroadcast: Broadcast[String] = null
  var _activeBroadcast: Broadcast[Boolean] = null

  var _nBatchsAccumulator: Accumulator[Int] = null

  override def run(): Unit = {

    _viewBroadcast = ssc.sparkContext.broadcast[String](view)
    _activeBroadcast = ssc.sparkContext.broadcast[Boolean](active)

    initView()


    println(s"RUN Stream ${_viewBroadcast.value} in ${_nBatchsAccumulator.value} Batchs")

    val groupId = "lambda_stream"
    val topicsSet: Set[String] = topics.split(",").toSet
    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //Different GroupId to replicate same data proccessing
      "group.id" -> groupId.concat(Thread.currentThread().getId.toString),
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val kstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
    )

    val parseRecord: ConsumerRecord[String, String] => (String, Int) = (r:ConsumerRecord[String, String]) => {
      try {
        (r.key, r.value.toInt)
      } catch {
        case e: Exception => (r.key, 1)
      }
    }



    //Parse Karka Record
    val ds =  kstream
      .filter(record => record.key.nonEmpty)
      .map(parseRecord)

    //Save RAW
    saveRaw(ds)

    //Stream Processing
    process(ds)

    //Batch Control
    val batchFinishedKs: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("batch-ended"), kafkaParams)
    )
    batchFinishedEventHandler(batchFinishedKs)


  }

  def initView() :Unit = {
    //Initilialize View
    val redis = redisClient.getInstance
    redis.scan(0, new ScanParams().`match`(_viewBroadcast.value.concat("::*")))
      .getResult.asScala.foreach(k => redis.del(k))

    //Wait one Batch to start processing
    if(active) {
      _nBatchsAccumulator = ssc.sparkContext.accumulator[Int](0)
      redis.set(_viewBroadcast.value.concat("::active"), "1")
    } else {
      _nBatchsAccumulator = ssc.sparkContext.accumulator[Int](-1)
    }
    redis.close()
  }

  /**
    * Save in RAW only if active
    *
    */
  def saveRaw(ds: DStream[(String, Int)]):Unit = {

    val hdfsFolder = hdfs.newDir

    ds.foreachRDD(rdd => {
      if (!rdd.isEmpty() && _activeBroadcast.value) {
        rdd.saveAsTextFile(hdfsFolder + Instant.now().toEpochMilli)
      }
    })
  }


  /**
    * Always save on its RealTime View
    */
  def process(ds: DStream[(String, Int)]): Unit = {
    val redisKeyPrefix = _viewBroadcast.value.concat("::")
    // Redis client setup
    var redis: Jedis = null
    ds.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        rdd.foreachPartition(partitionOfRecords => {
          redis = redisClient.getInstance
          partitionOfRecords.foreach(record =>
            redis.incrBy(redisKeyPrefix.concat(record._1), record._2)
          )
          redis.close()
        })

      }

    })
  }

  def batchFinishedEventHandler(batchFinishedStream: InputDStream[ConsumerRecord[String, String]]): Unit = {

    batchFinishedStream.foreachRDD(rdd => {
      if (rdd.count() > 0) {
        val redis = redisClient.getInstance

        _nBatchsAccumulator.setValue(_nBatchsAccumulator.value + 1)
        println(s"Stream -> ${_viewBroadcast.value} is at ${_nBatchsAccumulator.value} Batchs")

        //Error en la primera iteracion se activa el dos por estar en 0
        if (_nBatchsAccumulator.value == 1 && !_activeBroadcast.value) {
          //Clear RealTimeView
          redis.scan(0, new ScanParams().`match`(_viewBroadcast.value.concat("::*")))
            .getResult.asScala.foreach(k => redis.del(k))

          //Activate
          redis.set(_viewBroadcast.value.concat("::active"), "1")
          println(s"Stream -> ${_viewBroadcast.value}  activated")
          _activeBroadcast.destroy()
          _activeBroadcast = ssc.sparkContext.broadcast[Boolean](true)

        } else if (_nBatchsAccumulator.value > 2) {
          //Desactivate
          println(s"Stream -> ${_viewBroadcast.value}  desactivated")
          _nBatchsAccumulator.setValue(0)
          redis.del(_viewBroadcast.value.concat("::active"))
          _activeBroadcast.destroy()
          _activeBroadcast = ssc.sparkContext.broadcast[Boolean](false)
        }



        redis.close()


      }
    })
  }

}
