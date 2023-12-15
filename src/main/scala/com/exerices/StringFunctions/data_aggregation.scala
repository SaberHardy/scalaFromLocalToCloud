package com.exerices.StringFunctions

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object data_aggregation {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Apply Aggregations").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    val spark = SparkSession.builder().getOrCreate()

    // Read the file
    val file_content = spark.read.format("csv").option("header", true)
      .load("dataFiles/bankTransaction/bank_transactions.csv")

    val df = file_content.select("TransactionID", "CustLocation", "CustAccountBalance")

    df.show()

    println("These are the data gotten from the data itself")

    df.groupBy("CustLocation").agg(functions.sum(col("CustAccountBalance"))).show()

    spark.stop()
  }
}
