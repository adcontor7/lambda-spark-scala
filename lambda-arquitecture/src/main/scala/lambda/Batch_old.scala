package lambda

import java.io.File
import java.nio.file.{Files, StandardCopyOption}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Batch_old {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)

    val d1 = new File("data/new").toPath
    val d2 = new File("data/master").toPath
    import java.io.IOException
    import java.nio.file.StandardCopyOption
    try
      Files.move(d1, d2, StandardCopyOption.REPLACE_EXISTING)
    catch {
      case e: IOException =>
        e.printStackTrace()
    }
    //Files.delete(d1)

    // Spark Configuration Object

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("kshool")
    val sc = new SparkContext(conf)

    val fileTxt = "data/master/*/"
    val fileRDD = sc.textFile(fileTxt)


    println(fileRDD.count())

    val nLinesWithSpark = fileRDD.filter(line => line.toString contains "spark").count()
    println(nLinesWithSpark)

    val linesTaked = fileRDD.take(5)
    //Crear otro RDD con la muestra
    val wordCounts = sc.parallelize(linesTaked.flatMap(line => line.toString.split(" "))).count()
    println(wordCounts)

  }
}
