package extract

import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import java.util.Calendar
import com.jcraft.jsch._
import scala.collection.mutable.ListBuffer

/**
  * Created by Dharani.Sugumar on 4/2/2019.
  */
class FileRead{

  def getConf: Configuration = {
    try {
      // Setting up Hadoop File System Configurations
      println(Calendar.getInstance().getTime() + " Starting Setting up hadoop Configuration")
      val coreSitePath = new Path("/etc/hadoop/conf.cloudera.hdfs/core-site.xml")
      val hdfsSitePath = new Path("/etc/hadoop/conf.cloudera.hdfs/hdfs-site.xml")
      val conf: Configuration = new Configuration()
      println(Calendar.getInstance().getTime() + " Completed Setting up hadoop Configuration")
      return conf
    }
    catch {
      case e: Exception => e.printStackTrace()
        println(Calendar.getInstance().getTime() + " ERROR " + e.printStackTrace())
        throw new Exception("HDFS File System Configuration failure, hence Exiting ...!")
      case e: Exception => e.printStackTrace()
        println(Calendar.getInstance().getTime() + " ERROR " + e.printStackTrace())
        throw new Exception("HDFS File System Configuration failure, hence Exiting ...!")
    }
  }


  def getFileSystem: FileSystem = {
    try {
      // Setting up Hadoop File System Configurations
      println(Calendar.getInstance().getTime() + " Starting Setting up hadoop File System")
      val fileSystem: FileSystem = FileSystem.get(getConf)
      println(Calendar.getInstance().getTime() + " Completing Setting up hadoop File System")
      return fileSystem
    }
    catch {
      case e: Exception => e.printStackTrace()
        println(Calendar.getInstance().getTime() + " ERROR " + e.printStackTrace())
        throw new Exception("HDFS File System initialization failure, hence Exiting ...!")
      case e: Exception => e.printStackTrace()
        println(Calendar.getInstance().getTime() + " ERROR " + e.printStackTrace())
        throw new Exception("HDFS File System initialization failure, hence Exiting ...!")
    }
  }

   def sourceFileExists: ListBuffer[String] = {

     var srcDir ="src/main/resources/files"
    try {

      var list = scala.collection.mutable.ListBuffer[String]()
      val filesList = FileSystem.get(new Configuration()).listFiles(new Path(srcDir), false)
      while (filesList.hasNext) {
        list += filesList.next().getPath.getName
      }
      val finalList= list.filter(a => a.contains(".json"))

      for(file <- finalList) {
        if (getFileSystem.exists(new org.apache.hadoop.fs.Path(srcDir + "/" + file))) {
          println(Calendar.getInstance().getTime() + s" The Source File $file exist in HDFS and HDFS to SFTP file transfer will be starting ...!")
//          val path =srcDir + "/" + file
//          val fileCall = new jsonRead(sparkExe)
//          fileCall.fileProcess(path)


        }
      }
      finalList
    }
    catch {
      case e: Exception => e.printStackTrace()
        println(Calendar.getInstance().getTime() + " ERROR " + e.printStackTrace())
        throw new Exception("Source File doesn't exist in HDFS location, hence Exiting ...!")
    }
  }

  def execute = sourceFileExists

}
