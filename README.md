# Project Title
This project is designed to show the steps to create a jar file,
and deploy it to hdfs cloud system.

## Installation

Clone the repository, to download

## Usage

1- If you are using IntelliJIdea Editor, here are the steps to build the jar file:

 a. Right-click on the project

 b. Click on Project Structure

 c. In the pop-up window select: Artifacts

 d. Click on the Plus button (+), select JAR.

 e. Select: from module with dependencies

 f. Select the main class that you want to build the JAR from

 j. Click OK, and OK

 h. Go to build, and Build Artifacts.

 i. Click Build

 >> *Congratulations, the JAR file has been built.*

## Deploy to the CLOUD

After uploading the .JAR file to the cloud, run this command:
> spark-submit --master local[*] --class package_name.class_name /path/to/jar/file/in-hadoop/file-name.jar

## IN HDFS:

To check the data from JAR file:
> hdfs dfs -ls /path/to/save/data/part-00000
> hdfs dfs -cat /path/to/save/data/part-00000

## <p align="center"> NOTES </p>


> For creating RDDs use:
>
> <p align="center">SparkConf and SparkContext </p>
> 
> - val sparkConf = new SparkConf().setAppName("Log Data").setMaster("local[*]")
> - val sparkContext = new SparkContext(sparkConf)

> For create Spark DataFrames use:
> 
> SparkSession
> 
> - val sparkConf = new SparkConf().setAppName("DataFrames").setMaster("local[*]")
> - val sparkContext = new SparkContext(sparkConf)
> - val sparkSession = SparkSession.builder.getOrCreate()
> - import sparkSession.implicits._
