package model.JsonProcess

import model.{SparkTable, Table}
import org.apache.spark.sql.types.{DecimalType, IntegerType, StringType, StructType}

/**
  * Created by Dharani.Sugumar on 4/2/2019.
  */
class Review extends Table with SparkTable{
  override val physicalName: String = "nyc_review"
  override val logicalName: String = "nyc_review"
  override val tableDescription = "Newyorker table for Review json"

  override val schema: StructType = new StructType()
    .add("review_id", StringType)
    .add("user_id", StringType)
    .add("business_id", StringType)
    .add("stars", IntegerType)
    .add("date", StringType)
    .add("text", StringType)
    .add("useful", IntegerType)
    .add("funny", IntegerType)
    .add("cool", IntegerType)


  override val tableColumns: Array[(String, String)] = schemaToColumns(schema)
  override val createTableOpts: String = "STORED AS PARQUET\nTBLPROPERTIES ('COLUMN_STATS_ACCURATE'='true', 'auto.purge'='true', 'parquet.compression'='SNAPPY')"

}
