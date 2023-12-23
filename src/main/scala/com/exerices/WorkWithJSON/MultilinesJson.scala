package com.exerices.WorkWithJSON

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object MultilinesJson {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("MultiLines").setMaster("local[*]")
    val spark = SparkSession.builder.config(sparkConf)
      //      .config("spark.sql.legacy.timeParserPolicy", "LEGACY") // or "CORRECTED"
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val json_data = spark.read.format("json")
      .load("dataFiles/datToJSON/file1.json")

    json_data.printSchema()
    json_data.show()
    json_data.persist()

    /*
    The result after print the schema
    +---+-------+--------+-------------+
    |Age|Country|Employee| Organization|
    +---+-------+--------+-------------+
    | 33|  India|    John|ABC Inclusive|
    +---+-------+--------+-------------+
    */
    // Now lets read multi-lines
    val multi_json_data = spark.read.format("json")
      .option("multiline", true)
      .load("dataFiles/datToJSON/file2.json")

    multi_json_data.printSchema()
    multi_json_data.show()
    multi_json_data.persist()

    spark.stop()
  }
}
