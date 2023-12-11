package com.exerices.transformations

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object removeDuplicates {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Remove Duplication").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("Error")

    val spark = SparkSession.builder().getOrCreate()

    val file_content = spark.read.format("csv")
      .option("header", true)
      .load("dataFiles/bankTransaction/bank_transactions.csv")

    val counted_data = file_content.count()
    println(s"All the data are: $counted_data")
    file_content.printSchema()

    val removed_duplicates = file_content.distinct().count()

    println(s"All the data are: $removed_duplicates")

  }

}
