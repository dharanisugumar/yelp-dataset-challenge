package common

import java.io.File
import java.sql.Connection
import java.util.{Date, UUID}

import com.typesafe.config.{Config, ConfigFactory}
import model.SparkExecutor
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark._

/**
  * Companion object to assist construction of AppContext
  */
object AppContext extends AppContextTrait {
}
/**
  * Set of basic application properties (common for all services)
  *
  * @param config             - application configuration
  */
case class AppContext(
                       config: Config,
                       getSparkExecutor: (Config) => SparkExecutor,
                       stopSparkContext: () => Boolean
                     )

trait AppContextTrait {
  @transient protected var _spark: SparkSession = _
  @transient protected var _impalaJdbcConnection: Connection = _
  val appID = new Date().toString + math.floor(math.random * 10E4).toLong.toString
  val sparkConf = new SparkConf().setAppName("Data_Engineering")
    //.set("spark.sql.warehouse.dir", "hdfs://namenode/sql/metadata/hive")
    .set("spark.sql.catalogImplementation","hive")
    .setMaster("local[*]")
    .setAppName("Hive Example")

  protected def getSparkSession(): SparkSession = SparkSession.builder()
    .config(sparkConf)
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()

  protected def getConfig(): Config = ConfigFactory.load()

  //change the option according to the type of client you are using.

  protected def setupSparkSession(spark: SparkSession): Boolean = {
    try {
      spark.sql("set hive.execution.engine=spark")
      spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
      spark.sql("set hive.scratchdir.lock=false")
      true
    } catch {
      /*Missing database*/
      case _: NoSuchDatabaseException => {
        println(s"Database does not exist!")
        true
      }
      case e: Exception => {
        e.toString() + " --> " + ExceptionUtils.getStackTrace(e)
        throw e
      }
    }
  }




  protected def getSparkExecutor(config: Config): SparkExecutor = {
    setupSparkSession( _spark)
    return new SparkExecutor(_spark)
  }



  protected def stopSparkContext(): Boolean = {
    if (_spark != null)
    {
      _spark.stop()
      //      System.clearProperty("spark.driver.port")
      _spark = null
    }
    true
  }



  def apply(): AppContext = {
    _spark = getSparkSession()
    lazy val config = getConfig()
    val businessTable= "nyc_business".trim
    val checkinTable= "nyc_checkin".trim
    val photoTable= "nyc_photo".trim
    val reviewTable= "nyc_review".trim
    val tipTable= "nyc_tip".trim
    val userTable= "nyc_user".trim

    new AppContext(
        config
      ,  getSparkExecutor
      , stopSparkContext
    )
  }
}
