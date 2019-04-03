package model

import model.query.SqlQuery
import model.query.SparkSqlQuery
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.catalyst.ScalaReflection
import scala.reflect.runtime.universe._
import scala.tools.nsc.interpreter._

/**
  * Created by Dharani.Sugumar on 4/2/2019.
  */
class SparkExecutor(spark: SparkSession) {
  type T_SqlQueryType <: SqlQuery
  type T_TableType <: Table
  import  spark.implicits._
  def getSparksession():SparkSession =
  {
    spark
  }

  def encoderForList[T <: Product : TypeTag](list:java.util.List[T]): Dataset[T] = {
    import spark.implicits._
    return spark.createDataset(list)
  }


  final def getDataFrameOnly(sqlStatement: String): DataFrame = {
    try {
      spark.sql(sqlStatement)
    }
    catch {
      case e: QueryExecutionException => {
        e.toString() + " --> " + ExceptionUtils.getStackTrace(e)
        spark.sparkContext.stop()
        throw e
      }
    }
  }

  final protected def executeSqlOnly(sqlStatement: String): Boolean = {
    getDataFrameOnly(sqlStatement)
    return true
  }

  def executeSql(sqlStatement: String): Boolean = {
    //logHeader(sqlQuery, stage)
    val ret = executeSqlOnly(sqlStatement)
    //logFooter(sqlQuery, stage)
    return ret
  }

  def dropTable(table: SparkTable): Boolean = {
    executeSql(table.sqlDropTable.sqlStatement)
  }

  def createTable(table: SparkTable): Boolean = {
    executeSql(table.sqlCreateTable.sqlStatement)
  }

  final def registerAsTempTable(df: DataFrame, viewName: String, stage: String): Boolean = {
    val logMessage = s"Registering DataFrame as temporary table ${viewName}"
    // logStart(logMessage, stage)
    df.createOrReplaceTempView(viewName)
    //logEnd(logMessage, stage)
    return true
  }

  final def getDataFrame(sqlQuery: T_SqlQueryType): DataFrame = {
    //logHeader(sqlQuery, stage)
    val df = getDataFrameOnly(sqlQuery.sqlStatement)
    //logFooter(sqlQuery, stage)
    return df
  }


}
