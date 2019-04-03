package model.JsonProcess

import model.{SparkTable, Table}
import org.apache.spark.sql.types._

/**
  * Created by Dharani.Sugumar on 4/2/2019.
  */
 class Business() extends Table with SparkTable {

  override val physicalName: String = "nyc_business"
  override val logicalName: String = "nyc_business"
  override val tableDescription = "Newyorker table for Business json"

  override val schema: StructType = new StructType()
    .add("business_id", StringType)
    .add("name", StringType)
    .add("address", StringType)
    .add("city", StringType)
    .add("state", StringType)
    .add("postal_code", StringType)
    .add("latitude", DecimalType(13,2))
    .add("longitude", DecimalType(13,2))
    .add("stars", DecimalType(13,2))
    .add("review_count", StringType)
    .add("is_open", LongType)
    .add("Monday", StringType)
    .add("Tuesday", StringType)
    .add("Wednesday", StringType)
    .add("Thursday", StringType)
    .add("Friday", StringType)
    .add("Saturday", StringType)
    .add("Sunday", StringType)
    .add("categories",StringType)
    .add("RestaurantsReservations", StringType)
    .add("GoodForMeal", StringType)
    .add("BusinessParking", StringType)
    .add("Caters", StringType)
    .add("NoiseLevel", StringType)
    .add("RestaurantsTableService", StringType)
    .add("RestaurantsTakeOut", StringType)
    .add("RestaurantsPriceRange2", StringType)
    .add("OutdoorSeating", StringType)
    .add("BikeParking", StringType)
    .add("Ambience", StringType)
    .add("HasTV", StringType)
    .add("WiFi", StringType)
    .add("GoodForKids", StringType)
    .add("Alcohol", StringType)
    .add("RestaurantsAttire", StringType)
    .add("RestaurantsGoodForGroups", StringType)
    .add("RestaurantsDelivery", StringType)

  override val tableColumns: Array[(String, String)] = schemaToColumns(schema)
  override val createTableOpts: String = "STORED AS PARQUET\nTBLPROPERTIES ('COLUMN_STATS_ACCURATE'='true', 'auto.purge'='true', 'parquet.compression'='SNAPPY')"

}
