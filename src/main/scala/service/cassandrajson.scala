package service

import org.apache.spark.sql.SparkSession

/**
  * Created by Dharani.Sugumar on 4/1/2019.
  */
object cassandrajson extends App{
  val spark = SparkSession.builder().appName("Data Engineering").master("local[*]")
    .config("spark.cassandra.connection.host", "10.200.44.113")
              //.config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
              //.config("spark.cassandra.connection.host", "localhost")
              //.config("spark.cassandra.connection.port", "9042")
  .getOrCreate()
  val path ="C:\\Users\\dharani.sugumar\\Documents\\Resume\\Preparation\\New\\newyorker\\business.json"
  val df = spark.read.json(path)

  spark.sql("""CREATE TEMPORARY VIEW words
              |USING org.apache.spark.sql.cassandra
              |OPTIONS (
              |  table "words",
              |  keyspace "test")""".stripMargin)

  df.write.format("org.apache.spark.sql.cassandra")
    .options(Map("keyspace" -> "test", "table" -> "words")).mode("append").save()



  //  spark
//    .read
//    .format("org.apache.spark.sql.cassandra")
//    .options(Map("keyspace" -> "test", "table" -> "words"))
//    .load
//    .createOrReplaceTempView("words")
//  val rowsDataset= spark.sqlContext.read.format("org.apache.spark.sql.cassandra").option("keyspace", "myschema")
//    .option("table", "mytable").load()
//  rowsDataset.show()
}
