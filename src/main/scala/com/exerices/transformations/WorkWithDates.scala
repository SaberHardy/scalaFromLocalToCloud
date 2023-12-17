package com.exerices.transformations

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object WorkWithDates {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Apply Aggregations").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    val spark = SparkSession.builder().getOrCreate()

    // Read the file
    val file_content = spark.read.format("csv").option("header", true)
      .load("dataFiles/bankTransaction/bank_transactions.csv")
    file_content.persist()

    file_content.printSchema()

    file_content.show()

    val df = file_content.select("TransactionID", "CustLocation", "CustAccountBalance")
    df.show()

    df.withColumn("Current Date", current_date)
      .withColumn("Current Date", date_format(col("Current Date"), "MM/dd/yyyy"))
      .show()

    /* Show the aggregation functions */
    val new_df = df.groupBy("CustLocation")
      .agg(
        sum("CustAccountBalance").as("Total Balance"),
        count("TransactionID").as("IDs Exists"),
        max("CustAccountBalance").as("Max Balance"),
        min("CustAccountBalance").as("Min Balance"),
        avg("CustAccountBalance").as("Average Balance")
      )

    new_df.show(false)

    df.unpersist()
    spark.stop()

  }
}
