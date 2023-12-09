> *Spark DataFrames*
In Apache Spark, a DataFrame is a distributed collection of data,
> organized into named columns. It is similar to a table in 
> a relational database or a data frame in R or Python's pandas library. 
> Spark DataFrames provide a programming interface 
> for working with structured and semi-structured data.

> Case Class In Scala

* A case class is a regular class with some additional features.
* It is primarily used in regular Scala programming and is not specific to Spark.

_> A case class is a regular class with some additional features.

_> It is primarily used in regular Scala programming and is not specific to Spark.

> StructType (Spark SQL)

* StructType is a part of Spark SQL and is used to define the schema of a DataFrame.
* StructType is not focused on immutability. It is used to describe the structure of the data rather than instances of data.

---------------------------------------------------------------------------
## <p align="center"> Conclusion </p>
* In summary, case classes are more geared towards defining instances 
of structured data in a regular Scala program, while StructType 
is used for defining the structure of data within the context of
Spark SQL and DataFrames. Depending on your use case, you might 
use one or both in a Spark application.

## NOTE:
> For creating spark sparkContext you need spark config
> if you see an error that's mean you have double session created in the program
> 1. Create sparkConf
> 2. Create sparkContext
> 3. sparkContext.setLogLevel("Error")
> 4. val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
> 5. val read_csv_file = sparkSession.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("dataFiles/train.csv")

*  This will allow you to print all the results after the logs
> 6. Start the processing staff after this