package model

import model.query.SparkSqlQuery
import org.apache.spark.sql.types.StructType

/**
  * Created by Dharani.Sugumar on 4/2/2019.
  */
trait SparkTable extends Table {
  override type T_SqlType = SparkSqlQuery

  override lazy val sqlCreateTable = new SparkSqlQuery {
    val sqlStatement = sqlCreateTableStatement
    val logMessage = sqlCreateTableLogMessage
  }

  override lazy val sqlDropTable = new SparkSqlQuery {
    val sqlStatement = sqlDropTableStatement
    val logMessage = sqlDropTableLogMessage
  }

  override lazy val sqlTruncateTable = new SparkSqlQuery {
    val sqlStatement = sqlTruncateTableStatement
    val logMessage = sqlTruncateTableLogMessage
  }

  override def sqlInsertTable(sqlQuery: SparkSqlQuery) = new SparkSqlQuery {
    val sqlStatement = sqlInsertTableStatement(sqlQuery.sqlStatement)
    val logMessage = sqlInsertTableLogMessage(sqlQuery.logMessage)
  }

  def schemaToColumns(schema: StructType) = {
    schema.fields.view.map(field => (field.name, field.dataType.simpleString)).toArray
  }
}
