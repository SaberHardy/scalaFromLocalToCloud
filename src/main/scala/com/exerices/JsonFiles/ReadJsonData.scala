package com.exerices.JsonFiles

import org.apache.spark.sql.SparkSession

object ReadJsonData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Read Avro Data")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val read_file = spark.read.format("json")
      .option("header", "true")
      .option("multiLine", true) // This should be specified for reading multi-lines
      .load("dataFiles/random_user.json")

    read_file.printSchema()

    spark.stop()
  }
}
