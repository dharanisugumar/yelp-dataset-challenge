package extract

import java.io.{BufferedInputStream, FileNotFoundException, OutputStream}
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import java.util.Calendar

import com.jcraft.jsch._
import org.apache.hadoop.io.IOUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/**
  * Created by Dharani.Sugumar on 4/2/2019.
  */
class FileRead(srcDir:String,tgtDir:String){

  def getConf: Configuration = {
    try {
      // Setting up Hadoop File System Configurations
      println(Calendar.getInstance().getTime() + " Starting Setting up hadoop Configuration")
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

  val sftpHost = "secureftp.imshealth.com"
  val sftpUser = "yelp"
  val SFTP_ROOT_DIR= "yelp dir"
  val SFTP_PASSWORD = "sftp.pwd"
  val HDFS_CURRENT_USER = "dharani"
  /**
    * Method definition to open SFTP Channel
    *
    * @return
    */
  def getSftpChannel: ChannelSftp = {
    try {
      val jsch: JSch = new JSch()
      var session: Session = null
      println(Calendar.getInstance().getTime() + " SFTP Connectivity check is starting now ...!")
      println(Calendar.getInstance().getTime() + " SFTP Host is -> " + sftpHost)
      println(Calendar.getInstance().getTime() + " SFTP User is -> " + sftpUser)
      session = jsch.getSession(sftpUser, sftpHost, 22)
      session.setConfig("StrictHostKeyChecking", "no")
      val sftpPassword = PwdReader.Get("Challenge Application", HDFS_CURRENT_USER+SFTP_PASSWORD )
      session.setPassword(sftpPassword)
      session.connect()

      val channel: Channel = session.openChannel("sftp")
      channel.connect()
      val sftpChannel: ChannelSftp = channel.asInstanceOf[ChannelSftp]
      println(Calendar.getInstance().getTime() + " SFTP Connectivity check is successful ...!")
      return sftpChannel
    }
    catch {
      case e: JSchException => e.printStackTrace()
        println(Calendar.getInstance().getTime() + " ERROR " + e.printStackTrace())
        throw new Exception("SFTP Channel establishment failure, hence Exiting ...!")
      case e: SftpException => e.printStackTrace()
        println(Calendar.getInstance().getTime() + " ERROR " + e.printStackTrace())
        throw new Exception("SFTP Channel establishment failure, hence Exiting ...!")
    }
  }

  /**
    * Method definition to Copy File from Source to Target
    */
  def getBis(fileToBeCopied:String): BufferedInputStream = {
    try {
      println(Calendar.getInstance().getTime() + " Starting buffered input stream ")
      val newestFile = fileToBeCopied
      val bis: BufferedInputStream = new BufferedInputStream(getSftpChannel.get(srcDir + "/" + newestFile))
      println(Calendar.getInstance().getTime() + " Completed buffered input stream process ")
      return bis
    }
    catch {
      case e: Exception => e.printStackTrace()
        println(Calendar.getInstance().getTime() + " ERROR " + e.printStackTrace())
        throw new Exception("SFTP Input Stream failure, hence Exiting ...!")
      case e: Exception => e.printStackTrace()
        println(Calendar.getInstance().getTime() + " ERROR " + e.printStackTrace())
        throw new Exception("SFTP Input Stream failure, hence Exiting ...!")
    }
  }

   def copyFile(fileToBeCopied:ListBuffer[String]):ListBuffer[String] = {
    try {
      println(Calendar.getInstance().getTime() + " Starting HDFS File transfer ")
      val newestFile = fileToBeCopied
      for (fileCopy <- newestFile){
        val outputStream: OutputStream = getFileSystem.create(new Path(tgtDir +"/" + fileCopy))
        IOUtils.copyBytes(getBis(fileCopy), outputStream, getConf, true)
        println(Calendar.getInstance().getTime() + " Completed HDFS File transfer ")
        outputStream.close()
        println(" Completing copy process for file ..." + fileCopy)
      }
      println("############################## ---> Clean exit ##############################")
      fileToBeCopied
    } catch {
      case e: Exception => e.printStackTrace()
        println(Calendar.getInstance().getTime() + " ERROR " + e.printStackTrace())
        throw new Exception("SFTP File Transfer failure, hence Exiting ...!")
      case e: Exception => e.printStackTrace()
        println(Calendar.getInstance().getTime() + " ERROR " + e.printStackTrace())
        throw new Exception("SFTP File Transfer failure, hence Exiting ...!")
    }
  }

  def execute = sourceFileExists

}

object PwdReader {

  def Get(appName: String, filePath: String): String = {
    val conf = new SparkConf().setAppName(appName)
    val sc = SparkContext.getOrCreate(conf)
    //val spark = sparkExe.getSparksession()
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hadoopFS = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

    def getFromfile(path: String): Try[String] = {
      Try(sc.textFile(path).take(1)(0))
    }

    def validatePathAndGet(fileHDFSPath: String): String = {
      if (hadoopFS.exists(new org.apache.hadoop.fs.Path(fileHDFSPath))) {
        getFromfile(fileHDFSPath) match {
          case Success(str) => str
          case Failure(failure) => {
            println("Failed.." + failure.printStackTrace)
            throw new Exception("Failed..")
            //System.exit(1)
            ""
          }
        }
      }
      else {
        println(fileHDFSPath + " does not exist. hence exiting...")
        throw new FileNotFoundException(fileHDFSPath + " does not exist. hence exiting...")
        //System.exit(1)
        ""
      }
    }

    validatePathAndGet(filePath)
  }


}

