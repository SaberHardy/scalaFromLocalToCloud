package com.exerices.WorkWithJSON

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, struct}

object NestedData {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("MultiLines")
      .setMaster("local[*]")

    val spark = SparkSession.builder.config(sparkConf)
      // .config("spark.sql.legacy.timeParserPolicy", "LEGACY") // or "CORRECTED"
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val json_data = spark.read.format("json")
      .option("multiline", true)
      .load("dataFiles/datToJSON/address.json")
    json_data.persist()

    json_data.printSchema()
    json_data.show()

    val selected_data = json_data.select(
      "orgname",
      "Employee",
      "address.permanent_address",
      "address.temporary_address"
    )

    selected_data.persist()
    selected_data.show()

    val nested_data_from_json = selected_data.select(
      col("orgname"),
      col("Employee"),
      struct(
        col("permanent_address"),
        col("temporary_address")).alias("Address")
    )

    nested_data_from_json.printSchema()
    nested_data_from_json.show()

    spark.stop()
  }
}
