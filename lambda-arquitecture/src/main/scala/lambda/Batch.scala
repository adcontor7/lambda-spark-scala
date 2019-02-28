package lambda

import java.io.BufferedInputStream
import java.net.URI
import java.time.Instant

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileAlreadyExistsException, FileSystem, FileUtil, Path}
import org.apache.spark.{SparkConf, SparkContext}

class Batch(conf: SparkConf, view: String) extends Runnable{

  override def run(): Unit = {

    val spark = new SparkContext(conf)

    val rawDir = "hdfs://localhost/raw/";

    val hadoopConf = new Configuration()
    val hdfs = FileSystem.get(new URI("hdfs://localhost/"), hadoopConf)

    val inStream =new Path("hdfs://localhost/new/")
    val outFileStream = new Path(rawDir)

    if ( hdfs.exists( inStream ) ) {
      val status = hdfs.listStatus(inStream)
      status.foreach(inputSrc => {
        hdfs.rename(
          inputSrc.getPath,
          new Path(inputSrc.getPath.toString.replace("new","raw"))
        )
      })

    }
    else {
      // TODO - error logic here
    }

    val rdd = spark.wholeTextFiles(rawDir)
    rdd.foreach(f => println(f.toString))


  }
}
