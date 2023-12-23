package com.exerices.WorkWithJSON

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

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



    spark.stop()
  }
}
