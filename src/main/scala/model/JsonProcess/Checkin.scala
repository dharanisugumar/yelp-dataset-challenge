package model.JsonProcess

import model.{SparkTable, Table}
import org.apache.spark.sql.types._

/**
  * Created by Dharani.Sugumar on 4/2/2019.
  */
class Checkin() extends Table with SparkTable{
  override val physicalName: String = "nyc_checkin"
  override val logicalName: String = "nyc_checkin"
  override val tableDescription = "Newyorker table for Checkin json"

  override val schema: StructType = new StructType()
    .add("business_id", StringType)
    .add("date", StringType)


  override val tableColumns: Array[(String, String)] = schemaToColumns(schema)
  override val createTableOpts: String = "STORED AS PARQUET\nTBLPROPERTIES ('COLUMN_STATS_ACCURATE'='true', 'auto.purge'='true', 'parquet.compression'='SNAPPY')"

}
