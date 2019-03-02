package lambda

import java.net.URI
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileAlreadyExistsException, FileSystem, FileUtil, Path}
import org.apache.hadoop.mapred.InvalidInputException
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.{Jedis, ScanParams}
import scala.collection.JavaConverters._


class Batch(sc: SparkContext) extends Runnable{

  override def run(): Unit = {


    val hadoopConf = new Configuration()
    val fs = FileSystem.get(new URI("hdfs://localhost/"), hadoopConf)

    val newDataPath = new Path("hdfs://localhost/new/")
    val rawDataSet = "hdfs://localhost/raw/*/*"



    val props = new util.HashMap[String, Object]()

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    val kProducer = new KafkaProducer[String, String](props)
    val endMessage = new ProducerRecord[String, String]("batch-ended", "OK")

    while (true) {
      println("Start Batch processing")
      getNewData(fs, newDataPath)

      //Clear Batch view
      val redisClient = new Jedis("localhost", 6379) //TODO-TD USE POOL
      redisClient.scan(0, new ScanParams().`match`("BView::*"))
        .getResult.asScala.foreach(k => redisClient.del(k))
      redisClient.close()


      process(sc, rawDataSet)
      //Emulate Batch Processing time is up to 5s
      Thread.sleep(5000)

      kProducer.send(endMessage)
      //Sleep 5 seconds until RTViews switch
      Thread.sleep(5000)
    }

  }

  def getNewData(fs: FileSystem, inStream: Path): Unit = {
    if ( fs.exists( inStream ) ) {
      fs.listStatus(inStream).foreach(inputSrc => {
        fs.rename(
          inputSrc.getPath,
          new Path(inputSrc.getPath.toString.replace("new","raw"))
        )
      })
    }
    else {
      // TODO - error logic here
    }
  }

  def process(sc: SparkContext, rawDataSet: String): Unit = {


    val splitToTuple: String => (String, Int) = (s: String) => {
      val toRemove = "()".toSet
      val clean = s.filterNot(toRemove)
      val Array(k,v) = clean.split(",")
      ("BView::".concat(k), v.toInt)
    }

    var redisClient: Jedis = null
    try {
      sc.textFile(rawDataSet)
        .map(splitToTuple)
        .reduceByKey(_ + _) //Sum same key values
        .foreachPartition(partitionOfRecords => {
          //NOTE : EACH PARTITION ONE CONNECTION (more better way is to use connection pools)
          redisClient = new Jedis("localhost", 6379)
          //Batch size of 1000 is used since some databases cant use batch size more than 1000 for ex : Azure sql
          partitionOfRecords.grouped(1000).foreach(group => {
            group.foreach(record => {
              println(record)
              redisClient.set(record._1.toString, record._2.toString)
            })
          })
          redisClient.close()
        })
    } catch {
      case e: InvalidInputException => System.out.println("RAW is empty")
    }
  }
}
