package model.query

/**
  * Created by Dharani.Sugumar on 4/2/2019.
  */
abstract class SqlQuery() {
  val logMessage: String
  val sqlStatement: String
}
