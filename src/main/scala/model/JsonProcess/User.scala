package model.JsonProcess

import model.{SparkTable, Table}
import org.apache.spark.sql.types.{DecimalType, IntegerType, StringType, StructType}

/**
  * Created by Dharani.Sugumar on 4/2/2019.
  */
class User extends Table with SparkTable{
  override val physicalName: String = "nyc_user"
  override val logicalName: String = "nyc_user"
  override val tableDescription = "Newyorker table for User json"

  override val schema: StructType = new StructType()
    .add("user_id", StringType)
    .add("name", StringType)
    .add("review_count", IntegerType)
    .add("yelping_since", StringType)
    .add("friends", StringType)
    .add("useful", IntegerType)
    .add("funny", IntegerType)
    .add("cool", IntegerType)
    .add("fans", IntegerType)
    .add("year", IntegerType)
    .add("average_stars", DecimalType(13,2))
    .add("compliment_hot", IntegerType)
    .add("compliment_more", IntegerType)
    .add("compliment_profile", IntegerType)
    .add("compliment_cute", IntegerType)
    .add("compliment_list", IntegerType)
    .add("compliment_note", IntegerType)
    .add("compliment_plain", IntegerType)
    .add("compliment_cool", IntegerType)
    .add("compliment_funny", IntegerType)
    .add("compliment_writer", IntegerType)
    .add("compliment_photos", IntegerType)

  override val tableColumns: Array[(String, String)] = schemaToColumns(schema)
  override val createTableOpts: String = "STORED AS PARQUET\nTBLPROPERTIES ('COLUMN_STATS_ACCURATE'='true', 'auto.purge'='true', 'parquet.compression'='SNAPPY')"

}

