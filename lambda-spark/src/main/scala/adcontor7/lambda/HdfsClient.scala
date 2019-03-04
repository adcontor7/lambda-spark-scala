package adcontor7.lambda

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


class HdfsClient(root: String = "hdfs://localhost/") {

  var fs: FileSystem = null

  def newDir: String = root.concat("new/")
  def rawDir: String = root.concat("raw/")
  def rawDataSet: String = rawDir.concat("*/*")

  def getInstance: FileSystem = {
    if (fs != null){
      fs
    }
    val hadoopConf = new Configuration()
    fs = FileSystem.get(new URI(root), hadoopConf)
    fs
  }


  def mvNewData(): Unit = {

    val fs = getInstance
    val inStream = new Path(newDir)


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



}
