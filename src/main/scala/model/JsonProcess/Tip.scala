package model.JsonProcess

import model.{SparkTable, Table}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

/**
  * Created by Dharani.Sugumar on 4/2/2019.
  */
class Tip extends Table with SparkTable{
  override val physicalName: String = "nyc_tip"
  override val logicalName: String = "nyc_tip"
  override val tableDescription = "Newyorker table for Tip json"

  override val schema: StructType = new StructType()
    .add("user_id", StringType)
    .add("business_id", StringType)
    .add("text", StringType)
    .add("date", StringType)
    .add("compliment_count", IntegerType)

  override val tableColumns: Array[(String, String)] = schemaToColumns(schema)
  override val createTableOpts: String = "STORED AS PARQUET\nTBLPROPERTIES ('COLUMN_STATS_ACCURATE'='true', 'auto.purge'='true', 'parquet.compression'='SNAPPY')"

}
