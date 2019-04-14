package service

import java.util.Calendar

import common.service.SparkServiceTrait
import extract.FileRead
import model.JsonProcess._
import model.{SparkExecutor, SparkTable}
import model.query.SparkSqlQuery
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import spray.json


import scala.io.Source

/**
  * Created by Dharani.Sugumar on 4/1/2019.
  */
class jsonRead(sparkExecutor:SparkExecutor) {


  val spark = sparkExecutor.getSparksession()
  import spark.implicits._
  private lazy val businessTable = new Business
  private lazy val checkinTable = new Checkin
  private lazy val photoTable = new Photo
  private lazy val reviewTable = new Review
  private lazy val tipTable = new Tip
  private lazy val userTable = new User

  def parsejson(srcPath:String,tgtPath:String):DataFrame={

    val res = Source.fromFile(srcPath).getLines.//map{line=>line.replaceAll("'","").replaceAll("\"","").replace("{","{\"").replace(",","\",\"").replace(":","\":").replace("\":",":\"")}.toSeq
      map{line=>line.replaceAll("'","").replaceAll("\"","").replace("{","{\"").replace(",","\",\"").replace(":","\":").replace("\":","\":\"").replace("}","\"}").replace("\"{\"","{\"").replace("\"}\"","\"}")
      .replace("\":\"0",":0").replace(" ","")
    }.toSeq
    val yelpDS = spark.createDataset[String](res)(Encoders.STRING)
    yelpDS.coalesce(1).write.mode(SaveMode.Overwrite).json(s"${tgtPath}TestMisDay.json")
    yelpDS.show(2,false)
    val rdd = spark.read.textFile(s"${tgtPath}TestMisDay.json").toJSON.map(value => value.replace("\\", "").replace("{\"value\":\"", "").replace("}\"}", "}")).rdd
    val df = spark.read.json(rdd)
    df.printSchema()
    df
    //    df.filter($"attributes.BusinessParking".isNotNull).filter(!($"attributes.BusinessParking"==="None"))//.select("attributes.BusinessParking.garage").show(false)
    //      .show(false)
    //    val cjmDf =
    //      //spark.read.json(cjmDS)
    //    spark.read.schema(busSchema).json(rdd).as[Businessjson]
    //    cjmDf.printSchema()
    //    cjmDf.show(2,false)//select("attributes.BusinessParking.element.garage").show(false)


    //    //val samdf = spark.read.textFile("C:\\Users\\dharani.sugumar\\Documents\\Innovation\\com-newyorker-challenge\\src\\main\\resources\\files\\business.json")
    //    var json: String = ""
    //    //val data = this.getClass.getResourceAsStream("C:\\Users\\dharani.sugumar\\Documents\\Innovation\\com-newyorker-challenge\\src\\main\\resources\\files\\business.json")
    //    for (line <- Source.fromFile(fname).getLines) json += line
    //    //val lines = Source.fromFile(data).getLines.toSeq
    //    //val ds = spark.createDataset[String](lines)(Encoders.STRING)
    //
    //    val option = try {
    //      JSON.parseFull(json)
    //    } catch {
    //      case ex: Exception => ex.printStackTrace()
    //    }
    //
    //    // See what we found.
    //    option match {
    //      case None           => println("observations JSON invalid")
    //      case Some(elements) => println("observations JSON valid")
    //    }

  }

  def fileProcess(path: String,df:DataFrame) = {

    val fileFullName = path.substring(path.lastIndexOf("/") + 1).trim
    val fileExtract = fileFullName.substring(fileFullName.lastIndexOf(".")).length
    println(s" $fileFullName,$fileExtract and ${fileFullName.length - fileExtract}")
    val fileName = fileFullName.substring(0, fileFullName.length - fileExtract)
    println("started processing")
    spark.sqlContext.sql("use default")
    fileName match {
      case "business" => {
        println(s"creating table for ${fileName}")
        val tableName = businessTable
        //spark.sqlContext.sql("drop table default.nyc_business").show(false)
        sparkExecutor.createTable(tableName)
        df.select("business_id","name","address","city","state","postal_code","latitude","longitude","stars","review_count",
          "is_open", "hours.Monday","hours.Tuesday","hours.Wednesday","hours.Thursday","hours.Friday","hours.Saturday","hours.Sunday",
          "categories", "attributes.RestaurantsReservations","attributes.GoodForMeal.dessert","attributes.BusinessParking.garage",
          "attributes.Caters","attributes.NoiseLevel","attributes.RestaurantsTableService",
          "attributes.RestaurantsTakeOut","attributes.RestaurantsPriceRange2","attributes.OutdoorSeating",
          "attributes.BikeParking","attributes.Ambience","attributes.HasTV",
          "attributes.WiFi","attributes.GoodForKids","attributes.Alcohol",
          "attributes.RestaurantsAttire","attributes.RestaurantsGoodForGroups","attributes.RestaurantsDelivery")
          .withColumnRenamed("hours.Monday","Monday").withColumnRenamed("hours.Tuesday","Tuesday")
          .withColumnRenamed("hours.Wednesday","Wednesday").withColumnRenamed("hours.Thursday","Thursday")
          .withColumnRenamed("hours.Friday","Friday").withColumnRenamed("hours.Saturday","Saturday")
          .withColumnRenamed("hours.Sunday","Sunday").withColumnRenamed("attributes.RestaurantsReservations","RestaurantsReservations")
          .withColumnRenamed("attributes.GoodForMeal","GoodForMeal").withColumnRenamed("attributes.BusinessParking","BusinessParking")
          .withColumnRenamed("attributes.Caters","Caters").withColumnRenamed("attributes.NoiseLevel","NoiseLevel")
          .withColumnRenamed("attributes.RestaurantsTableService","RestaurantsTableService").withColumnRenamed("attributes.RestaurantsTakeOut","RestaurantsTakeOut")
          .withColumnRenamed("attributes.RestaurantsPriceRange2","RestaurantsPriceRange2").withColumnRenamed("attributes.OutdoorSeating","OutdoorSeating")
          .withColumnRenamed("attributes.BikeParking","BikeParking").withColumnRenamed("attributes.Ambience","Ambience").withColumnRenamed("attributes.HasTV","HasTV").withColumnRenamed("attributes.WiFi","WiFi")
          .withColumnRenamed("attributes.GoodForKids","GoodForKids").withColumnRenamed("attributes.Alcohol","Alcohol").withColumnRenamed("attributes.RestaurantsAttire","RestaurantsAttire").withColumnRenamed("attributes.RestaurantsGoodForGroups","RestaurantsGoodForGroups")
          .withColumnRenamed("attributes.RestaurantsDelivery","RestaurantsDelivery")
          .write.mode(SaveMode.Overwrite).insertInto(tableName.physicalName)
        println("started ************")
        spark.sqlContext.sql(s"select * from ${tableName.physicalName}").show(false)
      }
      case "checkin" => {
        println(s"creating table for ${fileName}")
        var tableName = checkinTable
        sparkExecutor.createTable(tableName)
        df.select("business_id","date").write.mode(SaveMode.Overwrite).insertInto(tableName.physicalName)
        //spark.sqlContext.sql(s"select * from ${tableName.physicalName}").show(false)
      }
      case "photo" => {
        var tableName = photoTable
        sparkExecutor.createTable(tableName)
        df.select("caption","photo_id","business_id","label").write.mode(SaveMode.Overwrite)
          .insertInto(tableName.physicalName)
        //spark.sqlContext.sql(s"select * from ${tableName.physicalName}").show(false)
      }
      case "review" => {
        var tableName = reviewTable
        sparkExecutor.createTable(tableName)
        df.select("review_id","user_id","business_id","stars","date","text","useful","funny","cool").write.mode(SaveMode.Overwrite)
          .insertInto(tableName.physicalName)
        //spark.sqlContext.sql(s"select * from ${tableName.physicalName}").show(false)
      }
      case "tip" => {
        var tableName = tipTable
        sparkExecutor.createTable(tableName)
        df.select("user_id","business_id","text","date","compliment_count").write.mode(SaveMode.Overwrite)
          .insertInto(tableName.physicalName)
        //spark.sqlContext.sql(s"select * from ${tableName.physicalName}").show(false)
      }
      case "user" => {
        var tableName = userTable
        sparkExecutor.createTable(tableName)
        df.select("user_id","name","review_count","yelping_since","friends","useful","funny","cool","fans",
          "elite","average_stars","compliment_hot","compliment_more","compliment_profile","compliment_cute",
          "compliment_list","compliment_note","compliment_plain","compliment_cool","compliment_funny",
          "compliment_writer","compliment_photos").write.mode(SaveMode.Overwrite)
          .insertInto(tableName.physicalName)
        //spark.sqlContext.sql(s"select * from ${tableName.physicalName}").show(false)
      }
      case _ => println("do nothing")
    }
  }
  def interestingQueries() = {

    val businessData = spark.table("default.nyc_business")
    val checkinData = spark.table("default.nyc_checkin")
    val photoData = spark.table("default.nyc_photo")
    val reviewData = spark.table("default.nyc_review")
    val tipData = spark.table("default.nyc_tip")
    val userData = spark.table("default.nyc_user")


    sparkExecutor.registerAsTempTable(businessData, "businessData", "businessData output")
    sparkExecutor.registerAsTempTable(checkinData, "checkinData", "checkinData output")
    sparkExecutor.registerAsTempTable(photoData, "photoData", "photoData output")
    sparkExecutor.registerAsTempTable(reviewData, "reviewData", "reviewData output")
    sparkExecutor.registerAsTempTable(tipData, "tipData", "tipData output")
    sparkExecutor.registerAsTempTable(userData, "userData", "userData output")

    println("Querying Process Started ************* ")
    println(" Query to list all the  review, photo, checkin, tip against business data ")

    val InterestingQuery1: SparkSqlQuery = new SparkSqlQuery {
      override val logMessage: String = "Data Querying"
      override val sqlStatement: String =
        s"""select photo.*,tip.*,checkin.*,review.* from businessData business
           |right join photoData photo
           |on business.business_id = photo.business_id
           |right join tipData tip
           |on tip.business_id = business.business_id
           |right join checkinData checkin
           |on checkin.business_id = business.business_id
           |right join reviewData review
           |on review.business_id = business.business_id""".stripMargin
    }
    val InterestingDf1 = sparkExecutor.getDataFrameOnly(InterestingQuery1.sqlStatement)
    InterestingDf1.show(false)

    println("Query to know the user who wrote the review and the review comments and user ratings on the products")
    val InterestingQuery2: SparkSqlQuery = new SparkSqlQuery {
      override val logMessage: String = "Data querying"
      override val sqlStatement: String =
        s"""select user.name,review.stars,review.date,review.text,review.useful,review.funny,review.cool,
           |user.friends,user.elite,user.average_stars,user.compliment_hot,user.compliment_more,user.compliment_profile,
           |user.compliment_cute,user.compliment_list,user.compliment_note,user.compliment_plain,user.compliment_cool,
           |user.compliment_funny,user.compliment_writer,user.compliment_photos
           |from userData user right join reviewData review on user.user_id = review.user_id
           |left join businessData business
           |on review.business_id = business.business_id""".stripMargin
    }
    val InterestingDf2 = sparkExecutor.getDataFrameOnly(InterestingQuery2.sqlStatement)
    InterestingDf2.show(false)

    println("Query to get the photo and business information against the business data")
    val InterestingQuery3: SparkSqlQuery = new SparkSqlQuery {
      override val logMessage: String = "Data querying"
      override val sqlStatement: String =
        s"""select photo.photo_id, photo.caption, photo.label, business.categories,business.RestaurantsTableService,
           |business.review_count,business.GoodForKids
           |from photoData photo right join businessData business on business.business_id = photo.business_id
           |and photo.business_id = business.business_id""".stripMargin
    }
    val InterestingDf3 = sparkExecutor.getDataFrameOnly(InterestingQuery3.sqlStatement)
    InterestingDf3.show(false)

    println("Query to list down the distinct list of categories for the business")
    val InterestingQuery4: SparkSqlQuery = new SparkSqlQuery {
      override val logMessage: String = "Data querying"
      override val sqlStatement: String =
        s"""select distinct(business.categories) as categories_list
           |from businessData business
         """.stripMargin
    }
    val InterestingDf4 = sparkExecutor.getDataFrameOnly(InterestingQuery4.sqlStatement)
    InterestingDf4.show(false)

    println("Query to get the review counts, stars, review comments on Saturdays and Sundays for the business city and state")
    val InterestingQuery5: SparkSqlQuery = new SparkSqlQuery {
      override val logMessage: String = "Data querying"
      override val sqlStatement: String =
        s"""select business.review_count,business.stars,business.Sunday, business.Saturday,review.text,
           |business.state, business.city
           |from businessData business
           |right join reviewData review on review.business_id =  business.business_id
         """.stripMargin
    }
    val InterestingDf5 = sparkExecutor.getDataFrameOnly(InterestingQuery5.sqlStatement)
    InterestingDf5.show(false)

    println("Query to get the years the user was elite")
    val InterestingQuery6: SparkSqlQuery = new SparkSqlQuery {
      override val logMessage: String = "Data querying"
      override val sqlStatement: String =
        s"""select user.elite as elite_user_year , user.user_id, user.name
           |from userData user
         """.stripMargin
    }
    val InterestingDf6 = sparkExecutor.getDataFrameOnly(InterestingQuery6.sqlStatement)
    InterestingDf6.show(false)

    println("Query to know the user who wrote the review and the review comments and user ratings on the products")
    val InterestingQuery7: SparkSqlQuery = new SparkSqlQuery {
      override val logMessage: String = "Data querying"
      override val sqlStatement: String =
        s"""
           |(select sum(review_count) over (partition by user.user_id) review_sum,
           |round(sum(review.stars) over (partition by user.user_id),2) review_stars,
           |round(avg(review.stars) over (partition by user.user_id),2) avg_review_stars,
           |rank() over (partition by user.user_id order by review.review_id desc) rnk,
           |dense_rank() over (partition by user.user_id order by review.review_id desc) dns_rnk,
           |percent_rank() over (partition by user.user_id order by review.review_id desc) pcnt_rnk,
           |row_number() over (partition by user.user_id order by review.review_id desc) rn
           |from userdata user join reviewData review
           |on user.user_id =  review.user_id)
           |where review_sum >=10
           |order by user.user_id,review.review_id

         """.stripMargin
    }
    val InterestingDf7 = sparkExecutor.getDataFrameOnly(InterestingQuery7.sqlStatement)
    InterestingDf7.show(false)


    println("Query to get the list of users who wrote the tips ")
    val InterestingQuery8: SparkSqlQuery = new SparkSqlQuery {
      override val logMessage: String = "Data querying"
      override val sqlStatement: String =
        s"""select count(user.user_id) user_count
           |from userData user left join tipData tip
           |on tip.user_id = user.user_id
           |group by user.user_id
         """.stripMargin
    }
    val InterestingDf8 = sparkExecutor.getDataFrameOnly(InterestingQuery8.sqlStatement)
    InterestingDf8.show(false)

    println("Query to get the no of working hours of the shop when the shop is open")
    //spark.read.table("default.nyc_business").withColumn("",substring(col("Monday"),,))
    val InterestingQuery9: SparkSqlQuery = new SparkSqlQuery {
      override val logMessage: String = "Data querying"
      override val sqlStatement: String =
        s"""select unix_timestamp(concat(substring(business.Monday,6,11),"00:00"),'HH:mm:ss') - unix_timestamp(oncat(substring(business.Monday,6,11),"00:00"),'HH:mm:ss'),
           |substring(business.Monday,6,11)-substring(business.Monday,5) as Monday_work_hours,
           |substring(business.Tuesday,6,11)-substring(business.Tuesday,5) as Tuesday_work_hours,
           |substring(business.Wednesday,6,11)-substring(business.Wednesday,5) as Wednesday_work_hours,
           |substring(business.Thursday,6,11)-substring(business.Thursday,5) as Thursday_work_hours,
           |substring(business.Friday,6,11)-substring(business.Friday,5) as Friday_work_hours,
           |substring(business.Saturday,6,11)-substring(business.Saturday,5) as Saturday_work_hours,
           |substring(business.Sunday,6,11)-substring(business.Sunday,5) as Sunday_work_hours
           |from businessData business
           |where business.is_open=1
         """.stripMargin
    }
    val InterestingDf9 = sparkExecutor.getDataFrameOnly(InterestingQuery9.sqlStatement)
    InterestingDf9.show(false)

    println("Query to find out which user has sent maximum number of reviews")
    val InterestingQuery10: SparkSqlQuery = new SparkSqlQuery {
      override val logMessage: String = "Data querying"
      override val sqlStatement: String =
        s"""select max(review_count),user_id as max_reviews
           |from userData user
           |group by user.user_id
         """.stripMargin
    }
    val InterestingDf10 = sparkExecutor.getDataFrameOnly(InterestingQuery10.sqlStatement)
    InterestingDf10.show(false)

    println("Querying process Ended ************* ")

  }

}

object jsonRead extends  jsonReadTrait

trait jsonReadTrait extends SparkServiceTrait {

  def loopJson() = {

    var list = scala.collection.mutable.ListBuffer[String]()
    val srcDir ="src/main/resources/files"
    val tgtDir ="src/main/resources/"
    val fileCall = new jsonRead(sparkExecutor)
    val filesList = FileSystem.get(new Configuration()).listFiles(new Path(srcDir), false)
    while (filesList.hasNext) {
      list += filesList.next().getPath.getName
    }
    val finalList = list.filter(a => a.contains(".json"))
    val numFiles = finalList.length
    println(s"Number of Files to be processed are:" + numFiles)
    if (numFiles > 0) {
      try {
        println(Calendar.getInstance().getTime() + " Started Reading Json files ")
        val fileTransfer = new FileRead(srcDir,tgtDir)
        for (file <- finalList) {
          if (fileTransfer.getFileSystem.exists(new org.apache.hadoop.fs.Path(srcDir + "/" + file))) {
            println(Calendar.getInstance().getTime() + s" The Source File $file exist in HDFS and HDFS to SFTP file transfer will be starting ...!")
            val path = srcDir + "/" + file
            fileTransfer.copyFile(finalList)
            val df = fileCall.parsejson(path,tgtDir)
            fileCall.fileProcess(path,df)
          }
        }
        fileCall.interestingQueries()
      }
      catch
        {
          case e: Exception => e.printStackTrace()
            println(Calendar.getInstance().getTime() + " ERROR " + e.printStackTrace())
            throw new Exception("Reading Json files failure, hence Exiting ...!")
        }
    }
    else {
      println("Hdfs path doesn't have any json files to initiate transfer")
    }
  }

  def main(args: Array[String]): Unit = {
    println("Started processing json files ************************")
    loopJson()
    println("File processing ended ***************************")

  }
}
