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
  
  Parsing Json data to Tabular format
  1. I used spark to read the json data => spark.read.json(path) => dataframe
  2. Reading nested objects inside the json file can be accessed using "attributes.RestaurantsReservations", "attributes.Caters" etc
  Parsed json data example in tabular format:
  
  
  3. I have created a data model for all the json present inside the directory.
  4. I had created a loop to read all the .json files inside the Untar directory. For every file, spark reads the json file and create a hive table and insert the table with the dataframe created.
  5. This step executes for all the files present inside the hadoop directory.
   
   Why hive?
  Using Hive as data store we can able to load JSON data into Hive tables by creating schemas. Easy to use. Same like sql-type language.

 Once all the tables are loaded, some of the intersting queries written on the data. I used corresponding maven dependencies to execute the process.
 pom dependencies: 
 1. Interesting query I => Query to list all the review, photo, checkin, tip against business data
  For the corresponding business_id from business.json, get reviews, photo, checking and tip details for the user (join photo.json,tip.json,review.json,checkin.json,user.json and business.json)
 2. Interesting query II => Query to know the user who wrote the review and the review comments and user ratings on the products
   For the business id, find out the user id , his review texts, ratings etc (join user.json, business.json, review.json)
 3. Interesting query III => Query to get the photo and business information against the business data
    Get the information on the photo details for the business
 4. Interesting query IV  => Query to list down the distinct list of categories for the business
    Get the unique distinct categories list for the corresponding business data
 5. Interesting query V => Query to get the review counts, stars, review comments on Saturdays and Sundays for the business city and         state
 For the business state and city, get the max review counts, starts received on peek days(saturdays and sundays)
 6. Interesting query VI => Query to get the years the user was elite
      List the years of the elite users
 7. Interesting query VII => Query to get the list of users who wrote the tips
      Get the count of users who wrote the tips
 8. Interesting query VIII => Query to get the no of working hours of the shop when the shop is open
      Get the working hours of all the days when the shop is open
 9. Interesting query IX => Query to find out which user has sent maximum number of reviews
      Find the user who has given the maximum reviews

 
   
  PreRequesities to run the docker script:
  1. Docker 
  2. Docker Spark
  3. Please go through this repository for deployment: https://github.com/dharanisugumar/yelp_docker_spark
   
  Execute the script using this command:  docker run -p 2222:22 --name yelp-challenge-app -e ENABLE_INIT_DAEMON=false --network docker-spark_default -v `pwd`:/opt --link spark-master:spark-master -t yelp/challenge-app /bin/bash /yelp_processor.sh yelp_dataset.tar
