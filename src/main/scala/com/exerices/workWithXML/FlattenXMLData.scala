package com.exerices.workWithXML

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}

object FlattenXMLData {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Flatten XML data")
      .setMaster("local[*]")

    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("Error")

    val spark = SparkSession
      .builder
      .getOrCreate()
    import spark.implicits._

    val xml_data = spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "catalog_item")
      .load("dataFiles/datToJSON/product.xml")

    xml_data.printSchema()
    xml_data.show()

    val size_array = xml_data.withColumn(
      "size", explode(col("size")))

    size_array.printSchema()
    size_array.show()

    println("---- Flatten all the columns ----")
    val selected_columns = size_array.select(
      col("_gender"),
      col("item_number"),
      col("price"),
      col("size.*")
    )
    selected_columns.printSchema()
    selected_columns.show()

    val flatt_color = selected_columns.withColumn(
      "color_swatch",
      explode(col("color_swatch"))
    )
    flatt_color.printSchema()

   val selected_data_columns = flatt_color.select(
     col("item_number"),
     col("_gender"),
     col("price"),
     col("_description"),
     col("color_swatch._VALUE"),
     col("color_swatch._image"),
   )

    selected_data_columns.printSchema()
    selected_data_columns.show(false)

  }
}
