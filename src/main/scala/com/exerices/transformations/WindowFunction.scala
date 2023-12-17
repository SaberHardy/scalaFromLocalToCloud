package com.exerices.transformations

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number

object WindowFunction {
  def main(args: Array[String]): Unit = {
    """
      |Window functions are typically used in Spark SQL or DataFrame API for analytical queries.
      | Some common use cases for window functions include:
      |  calculating running totals, moving averages, rank,
      |  and other aggregations over a specified window of rows.
      |""".stripMargin

    val sparkConf = new SparkConf().setAppName("Apply Aggregations").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    val spark = SparkSession.builder().getOrCreate()

    // Read the file
    val file_content = spark.read
      .format("csv")
      .option("header", true)
      .load("dataFiles/bankTransaction/bank_transactions.csv")
    file_content.persist()

    file_content.printSchema()

    file_content.show()

    val windowData = Window.partitionBy("CustomerID")
      .orderBy("CustAccountBalance")
    println("This is window function......!")

    file_content.select(
        "CustAccountBalance",
        "CustomerID",
        "TransactionDate")

      .withColumn("RowNumber",
        row_number().over(windowData)).show()

    spark.close()

  }
}