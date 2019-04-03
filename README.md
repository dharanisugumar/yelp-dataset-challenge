# yelp-dataset-challenge
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
  
  I have dockerized all the SMACK components and the program. Docker script gets the yelpdataset.tar(for eg) as a input. 
    Firt Step: untar the file and copy the files to the desired local folder
    Second Step: Places those files into HDFS. Spark runs on top on HDFS(for distributed file storage)
    Third Step: Invoke spark submit with the application jar
  
  Parsing Json data to Tabular format
  1. I used spark to read the json data => spark.read.json(path) => dataframe
  2. I have created a data model for all the json present inside the directory.
  3. I had created a loop to read all the .json files inside the untar directory. For every file, spark reads the json file and create the      table and insert the table with the dataframe created
  4. This step executes for all the files present inside the hadoop directory.
   
 Once all the tables are loaded, Some of the intersting queries written on the data.
 
   
   
