package com.exerices.JsonFiles

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object PassSchemaWhenReading {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Read Avro Data")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val dml = StructType(Array(
      StructField("state", StringType, true),
      StructField("capital", StringType, true),
      StructField("language", StringType, true),
      StructField("country_id", StringType, true)
    ))

    val read_file = spark.read.format("csv")
      .option("header", "false")
      .schema(dml)
      .load("dataFiles/countries*")

    read_file.show(100)
    read_file.coalesce(1)
  }

}
