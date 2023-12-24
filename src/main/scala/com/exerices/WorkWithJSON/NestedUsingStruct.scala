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

    val df1 = spark.read.format("json")
      .option("multiline", true)
      .load("dataFiles/datToJSON/cake.json")

    df1.persist()
    df1.printSchema()
    df1.show(false)

    // Flatten  the data using explode function

    val flatten_array_df = df1.withColumn(
      "topping",
      explode(col("topping")))

    println("Flatten Data by creating new  column")
    flatten_array_df.printSchema()
    flatten_array_df.show(false)

    println("Flatten Data rename id of topping after structure the data")

    val struct_df = flatten_array_df
      .withColumn("topping_id", col("topping.id"))
      .withColumn("topping_type", col("topping.type"))
      .drop(col("topping"))

    struct_df.printSchema()
    struct_df.show(false)


    val selected_data = struct_df.select(
      col("batters.*"),
      col("id"),
      col("name"),
      col("ppu"),
      col("type"),
      col("topping_id"),
      col("topping_type")
    )

    selected_data.printSchema()
    selected_data.show(false)

    val batter_data = selected_data
      .withColumn("batter", explode(col("batter")))
      .withColumn("element_id", col("batter.id"))
      .withColumn("element_type", col("batter.type"))
      .drop("batter")

    println("Flatten Data with Batter column")

    batter_data.printSchema()
    batter_data.show(false)
  }
}
