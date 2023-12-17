package com.exerices.transformations

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object PivotingData {
  """
    | Pivoting is basically transforming rows to
    | columns in data using ....
    |""".stripMargin

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Apply Aggregations").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    val spark = SparkSession.builder().getOrCreate()

    sparkContext.setLogLevel("Error")

    // Read the file
    val file_content = spark.read
      .format("csv")
      .option("header", true)
      .load("dataFiles/bankTransaction/bank_transactions.csv")

    file_content.persist()

    file_content.printSchema()
    file_content.show()

    val df = file_content.select(
      col("CustGender").as("Gender"),
      col("CustLocation").as("Location"),
      col("CustAccountBalance").cast("int").as("Balance"))
    println("This data is for selection")
    df.show(false)

    val pivotDF = df
      .groupBy("Location")
      .pivot("Gender")
      .sum("Balance")

    println("This data is for Pivoting.......")

    pivotDF.show(false)

    file_content.unpersist()
    spark.stop()
  }
}
