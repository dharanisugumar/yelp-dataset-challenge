package model.JsonProcess

import model.{SparkTable, Table}
import org.apache.spark.sql.types.{StringType, StructType}

/**
  * Created by Dharani.Sugumar on 4/2/2019.
  */
class Photo extends Table with SparkTable{
  override val physicalName: String = "nyc_photo"
  override val logicalName: String = "nyc_photo"
  override val tableDescription = "Newyorker table for Photo json"

  override val schema: StructType = new StructType()
    .add("caption", StringType)
    .add("photo_id", StringType)
    .add("business_id", StringType)
    .add("label", StringType)

  override val tableColumns: Array[(String, String)] = schemaToColumns(schema)
  override val createTableOpts: String = "STORED AS PARQUET\nTBLPROPERTIES ('COLUMN_STATS_ACCURATE'='true', 'auto.purge'='true', 'parquet.compression'='SNAPPY')"

}
