package com.exerices.WorkWithJSON

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, struct}

object GenerateComplexData {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("GenerateData")
      .setMaster("local[*]")
    val spark = SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val json_data = spark.read.format("json")
      .option("multiline", true)
      .load("dataFiles/datToJSON/address_flat.json")
    json_data.persist()

    json_data.printSchema()
    /*
    root
     |-- Employee: string (nullable = true)
     |-- orgname: string (nullable = true)
     |-- permanent_address: string (nullable = true)
     |-- temporary_address: string (nullable = true)
     *
     * TODO: The mission is to create a struct data
     * root
     |-- Employee: string (nullable = true)
     |-- orgname: string (nullable = true)
     |-- address: struct (nullable = true)
     |----- permanent_address: string (nullable = true)
     |----- temporary_address: string (nullable = true)
    */
    json_data.show()

    val structuredData = json_data.select(
      col("Employee"),
      col("orgname"),
      struct(
        col("permanent_address"),
        col("temporary_address")
      ).alias("Address")
    )
    structuredData.printSchema()
    structuredData.show()
    /*
    * root
     |-- Employee: string (nullable = true)
     |-- orgname: string (nullable = true)
     |-- Address: struct (nullable = false)
     |    |-- permanent_address: string (nullable = true)
     |    |-- temporary_address: string (nullable = true)
    * */

  }
}
