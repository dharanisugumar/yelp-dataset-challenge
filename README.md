yelp-dataset-challenge
Data Engineering task 

Using SMACK components, i need to parse the json data of the yelp data set (https://www.yelp.com/dataset_challenge/dataset) into a tabular format and write queries to validate the data. 

Technology/Infrastructure used:
  1. Spark 
  2. HDFS
  3. Apache Hive
  4. Scala programming
  5. Shell scripting
  6. Docker
  7. Linux
  8. Maven
  
  I have dockerized all the SMACK components and the program. Docker script gets the yelpdataset.tar (for eg) as a input. 
    
    First Step: Untar the file and copy the files to the desired local folder
    Second Step: Places those files into HDFS. Spark runs on top on HDFS (for distributed file storage)
    Third Step: Invoke spark submit with the application jar
 
   
  Installation: 
   
  PreRequesities to run the docker script:
  1. Docker 
  2. Docker Spark
  3. Please go through this repository for deployment: https://github.com/dharanisugumar/yelp_docker_spark
   
   Execute the script using this command:  
  docker run -p 2222:22 --name yelp-challenge-app -e ENABLE_INIT_DAEMON=false --network docker-spark_default -v `pwd`:/opt --link spark-master:spark-master -t yelp/challenge-app /bin/bash /yelp_processor.sh yelp_dataset.tar
  
    ![Alt text](https://github.com/dharanisugumar/yelp-dataset-challenge/blob/master/src/main/resources/scripts/docker-build-yelp.png "docker")
  
   ![Alt text](https://github.com/dharanisugumar/yelp-dataset-challenge/blob/master/src/main/resources/scripts/docker-run-command-output.png "docker-run")
   
  
  
  Parsing Json data to Tabular format
  1. I used spark to read the json data => spark.read.json(path) => dataframe
  2. Reading nested objects inside the json file can be accessed using "attributes.RestaurantsReservations", "attributes.Caters" etc
  3. I have created a data model for all the json present inside the directory.
  4. I had created a loop to read all the .json files inside the Untar directory. For every file, spark reads the json file and create a      hive table and insert the table with the dataframe created.
  5. This step executes for all the files present inside the hadoop directory.
   ![Alt text](Desktop/Capture.jpg?raw=true "Json")
   
   Why hive?
    Using Hive as data store we can able to load JSON data into Hive tables by creating schemas. Easy to use. Same like sql-type          language.

 Once all the tables are loaded, some of the intersting queries written on the data. I used corresponding maven dependencies to execute the process.
 
 Tables are created under default database in hive.
 1. nyc_business for business.json data
 2. nyc_checkin for checkin.json
 3. nyc_review for review.json
 4. nyc_user for user.json
 5. nyc_tip for tip.json
 6. nyc_photo for photo.json
 
 Creating a dataframe:
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
    
 Queries to Execute:

 1. Interesting query I => Query to list all the review, photo, checkin, tip against business data
  For the corresponding business_id from business.json, get reviews, photo, checking and tip details for the user (join photo.json,tip.json,review.json,checkin.json,user.json and business.json.  
  
select photo.*,tip.*,checkin.*,review.* from businessData business
right join photoData photo
on business.business_id = photo.business_id
right join tipData tip
on tip.business_id = business.business_id
right join checkinData checkin
on checkin.business_id = business.business_id
right join reviewData review
on review.business_id = business.business_id

 2. Interesting query II => Query to know the user who wrote the review and the review comments and user ratings on the products
   For the business id, find out the user id , his review texts, ratings etc (join user.json, business.json, review.json)
   
select user.name,review.stars,review.date,review.text,review.useful,review.funny,review.cool,
user.friends,user.elite,user.average_stars,user.compliment_hot,user.compliment_more,user.compliment_profile,
user.compliment_cute,user.compliment_list,user.compliment_note,user.compliment_plain,user.compliment_cool,
user.compliment_funny,user.compliment_writer,user.compliment_photos
from userData user right join reviewData review on user.user_id = review.user_id
left join businessData business
on review.business_id = business.business_id

 3. Interesting query III => Query to get the photo and business information against the business data
    Get the information on the photo details for the business
    
select photo.photo_id, photo.caption, photo.label, business.categories,business.RestaurantsTableService,
business.review_count,business.GoodForKids
from photoData photo right join businessData business on business.business_id = photo.business_id
and photo.business_id = business.business_id

 4. Interesting query IV  => Query to list down the distinct list of categories for the business
    Get the unique distinct categories list for the corresponding business data
    
select distinct(business.categories) as categories_list
from businessData business

 5. Interesting query V => Query to get the review counts, stars, review comments on Saturdays and Sundays for the business city and         state
 For the business state and city, get the max review counts, starts received on peek days(saturdays and sundays)
 
select business.review_count,business.stars,business.Sunday, business.Saturday,review.text,
business.state, business.city
from businessData business
right join reviewData review on review.business_id =  business.business_id

 6. Interesting query VI => Query to get the years the user was elite
      List the years of the elite users
      
select user.elite as elite_user_year , user.user_id, user.name
from userData user

 7. Interesting query VII => Query to get the list of users who wrote the tips
      Get the count of users who wrote the tips
      
select count(user.user_id) user_count
from userData user left join tipData tip
on tip.user_id = user.user_id
group by user.user_id

 8. Interesting query VIII => Query to get the no of working hours of the shop when the shop is open
      Get the working hours of all the days when the shop is open
      
select unix_timestamp(concat(substring(business.Monday,6,11),"00:00"),'HH:mm:ss') - unix_timestamp(oncat(substring(business.Monday,6,11),"00:00"),'HH:mm:ss'),
substring(business.Monday,6,11)-substring(business.Monday,5) as Monday_work_hours,
substring(business.Tuesday,6,11)-substring(business.Tuesday,5) as Tuesday_work_hours,
substring(business.Wednesday,6,11)-substring(business.Wednesday,5) as Wednesday_work_hours,
substring(business.Thursday,6,11)-substring(business.Thursday,5) as Thursday_work_hours,
substring(business.Friday,6,11)-substring(business.Friday,5) as Friday_work_hours,
substring(business.Saturday,6,11)-substring(business.Saturday,5) as Saturday_work_hours,
substring(business.Sunday,6,11)-substring(business.Sunday,5) as Sunday_work_hours
from businessData business
where business.is_open=1

 9. Interesting query IX => Query to find out which user has sent maximum number of reviews
      Find the user who has given the maximum reviews
      
select max(review_count),user_id as max_reviews
from userData user
group by user.user_id

