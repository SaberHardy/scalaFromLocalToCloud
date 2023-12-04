package com.exerices.RDDs

import org.apache.spark.{SparkConf, SparkContext}

case class CountryDefinition(
        countryName: String,
        countryState: String,
        countryLanguage: String,
        countryCode: String)

object SchemaRDDs {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Use Schema Rdd").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("Error")

    val file_content = sparkContext.textFile("dataFiles/countries.txt")
    val split_file = file_content.map(x => x.split(","))
    val split_input = split_file.map(x => CountryDefinition(x(0), x(1), x(2), x(3)))

    split_input.foreach(println)

  }
}
