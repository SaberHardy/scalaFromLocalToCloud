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
      StructField("state", StringType),
      StructField("capital", StringType),
      StructField("language", StringType),
      StructField("country_id", StringType)
    ))

    val read_file = spark.read.format("csv")
      .option("header", "false")
      .schema(dml)
      .load("dataFiles/countries*")


    read_file.coalesce(1)
      .write.mode("overwrite")
      .format("csv")
      .partitionBy("country_id")
      .save("dataFiles/partitioned_data")
  }

}
