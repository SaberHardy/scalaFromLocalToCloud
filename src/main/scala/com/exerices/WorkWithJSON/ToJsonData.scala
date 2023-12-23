package com.exerices.WorkWithJSON

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object ToJsonData {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("ToJsonData").setMaster("local[*]")
    val spark = SparkSession.builder.config(sparkConf)
      //      .config("spark.sql.legacy.timeParserPolicy", "LEGACY") // or "CORRECTED"
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    //    val dataFile = spark.read.format("text")
    //      .option("header", true)
    //      .option("inferSchema", true)
    //      .option("delimiter", "~")
    //      .load("dataFiles/datToJSON/file_data.txt")


    """
      |Let's see the format of the line of the data:
      |Name ==> "Paul"~
      |Age ==> "33"~
      |Address ==> {"city":"Hyderabad","state":"Telangana","country":"India"}
      |Note: The address field needs another format
      |city ==>
      |state ==>
      |Country ==>
      |
      |""".stripMargin
    val dml = StructType(
      Array(
        StructField("name", StringType, true),
        StructField("age", StringType, true),
        StructField("address", StringType, true)
      )
    )
    val jsonSchema = StructType(
      Array(
        StructField("city", StringType, true),
        StructField("state", StringType, true),
        StructField("country", StringType, true)
      )
    )

    val dataframe = spark.read.format("csv")
      .option("delimiter", "~")
      //      .option("header", false)
      .schema(dml)
      .load("dataFiles/datToJSON/file_data.txt")

    dataframe.printSchema()
    dataframe.show()
    dataframe.persist()


    /*
    The result after print the schema
    +-------+---+--------------------+
    |   name|age|             address|
    +-------+---+--------------------+
    |   Paul| 33|{"city":"Hyderaba...|
    |  Robin| 27|{"city":"chennai"...|
    |William| 44|{"city":"Mumbai",...|
    |  Pooja| 36|{"city":"Trivandr...|
    |  Rahul| 21|{"city":"Hyderaba...|
    |Jessica| 20|{"city":"Ahmadaba...|
    +-------+---+--------------------+
    * */

    val final_df = dataframe.select(
      col("name"),
      col("age"),
      from_json(col("address"), jsonSchema).as("Address"))

    final_df.printSchema()
    final_df.show(false)

    final_df.persist()

    //    Access to fields in the table
    val df_accessed = final_df.select(
      col("name"),
      col("age"),
      col("address.city"),
      col("address.state"),
      col("address.country"))

    df_accessed.printSchema()
    df_accessed.show(false)

    spark.stop()
  }
}
