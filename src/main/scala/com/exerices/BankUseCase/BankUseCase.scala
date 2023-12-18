package com.exerices.BankUseCase

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number, window}

object BankUseCase {

  """
    |---- TASK TODO ----
    |
    | 1. In each location, identify the record with the highest
    | Customer Account Balance
    | 2. In which location we have the most number of transaction
    | 3. Which location has the highest sum of total_transaction_amount
    |
    |""".stripMargin

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("UseCase")
      .setMaster("local[*]")

    val sparkContext = new SparkContext(sparkConf)
    val spark = SparkSession.builder().getOrCreate()

    sparkContext.setLogLevel("Error")

    // Read the file
    val bankDF = spark.read
      .format("csv")
      .option("header", true)
      .load("dataFiles/bankTransaction/bank_transactions.csv")

    bankDF.persist()
    bankDF.printSchema()

    val highestAccountBalance = Window.partitionBy("CustLocation")
      .orderBy(col("CustAccountBalance").cast("Int").desc_nulls_last,
        col("TransactionAmount (INR)").cast("Int").desc_nulls_last)

    val customerData = bankDF.withColumn("RowNumber",
      row_number().over(highestAccountBalance))
      .select("CustomerID", "CustAccountBalance",
        "TransactionAmount (INR)", "RowNumber")

    customerData.show()
  }
}