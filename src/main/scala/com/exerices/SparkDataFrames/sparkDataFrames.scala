package com.exerices.SparkDataFrames

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.{SparkConf, SparkContext}

case class SchemaCountries(
                            countryName: String,
                            countryState: String,
                            countryLanguage: String,
                            countryCode: String)

object sparkDataFrames {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DataFrames").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    val sparkSession = SparkSession.builder.getOrCreate()
    import sparkSession.implicits._

    sparkContext.setLogLevel("Error")

    val read_file = sparkContext.textFile("dataFiles/countries.txt")
    val split_filedata = read_file.map(x => x.split(","))

    val allData = split_filedata.map(x => SchemaCountries(x(0), x(1), x(2), x(3)))

    val english_data = allData.filter(x => x.countryLanguage.contains("English"))

    english_data.foreach(println)

    println("---------- Convert to Dataframe ------------")
    val english_dataframe = english_data.toDF()
    english_dataframe.foreach(println)

    println("---------- print Schema of Dataframe ------------")
    english_dataframe.printSchema()

    println("---------- Select Specific columns from Dataframe ------------")
    val allColumns_data = english_dataframe.select("countryName", "countryState", "countryLanguage", "countryCode")
    allColumns_data.show(false)

    println("---------- Select Specific columns with filter condition ------------")

    //    allColumns_data.filter("ColumnName == Specific condition")
    allColumns_data.filter("countryCode== 'IND' ").show()
    /*

    Result
    * ---------- Select Specific columns with filter condition ------------
    +-----------------+------------+---------------+-----------+
    |      countryName|countryState|countryLanguage|countryCode|
    +-----------------+------------+---------------+-----------+
    |Arunachal Pradesh|    Itanagar|        English|        IND|
    |        Meghalaya|    Shillong|        English|        IND|
    |         Nagaland|      Kohima|        English|        IND|
    +-----------------+------------+---------------+-----------+
    * */

    /* Another method is creating a temp table and query using sql queries */

    println("---------- Select using SQL condition ------------")
    english_dataframe.createOrReplaceTempView("CountrySqlTable")
    sparkSession.sql("select * from CountrySqlTable").show()

    /* Using Row RDDs */

    println("---------- Select using Struct ------------")

    val rowRdd = split_filedata.map(x => Row(x(0), x(1), x(2), x(3)))
    val struct_schema = StructType(
      Array(
        StructField("countryName", StringType, true),
        StructField("countryState", StringType, true),
        StructField("countryLanguage", StringType, true),
        StructField("countryCode", StringType, true)
      )
    )
    val struct_df = sparkSession.createDataFrame(rowRdd, struct_schema)
    struct_df.persist()

    struct_df.show(false)
    struct_df.printSchema()

    println("---------- Create a Temp view to query the data ------------")
    // Use persist to cache the data in the memory

    struct_df.createOrReplaceTempView("struct_country_table")

    sparkSession.sql("select * from struct_country_table").show()



  }

}
