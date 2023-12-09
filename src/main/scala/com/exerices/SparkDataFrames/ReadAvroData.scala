package com.exerices.SparkDataFrames

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ReadAvroData {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Read Avro Data").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("Error")

    val spark = SparkSession.builder().getOrCreate()
    val read_file = spark.read.format("text")
      .option("header", "true")
      .option("delimiter", ",")
      .load("dataFiles/countries.txt")

  }

}
