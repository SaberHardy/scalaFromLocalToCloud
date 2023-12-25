package com.exerices.workWithXML

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

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
  }
}
