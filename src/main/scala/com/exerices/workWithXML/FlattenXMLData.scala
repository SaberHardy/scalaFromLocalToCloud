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

    val size_array  =xml_data.withColumn(
      "Size", explode(col("size")))

    size_array.printSchema()
    size_array.show()
  }
}
