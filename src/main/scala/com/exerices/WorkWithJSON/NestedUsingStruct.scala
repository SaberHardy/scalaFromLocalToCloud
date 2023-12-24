package com.exerices.WorkWithJSON

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, struct}

object NestedUsingStruct {
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
      .load("dataFiles/datToJSON/cake.json")
    json_data.persist()
    json_data.printSchema()
    json_data.show()

    // Flatten  the data using explode function

    val flatten_df = json_data.withColumn(
      "topping",
      explode(col("topping")))

    println("Flatten Data by creating new  column")
    flatten_df.printSchema()
    flatten_df.show()

    println("Flatten Data rename id of topping after structure the data")

    val new_id_data = flatten_df.withColumn(
      "topping_id", col("topping.id"))
      .withColumn("topping_type", col("topping.type"))
      .drop("topping")

    json_data.printSchema()
    new_id_data.show()


  }
}
