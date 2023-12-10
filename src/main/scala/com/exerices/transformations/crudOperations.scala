package com.exerices.transformations
import org.apache.spark.sql.functions._

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object crudOperations {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Transformations").setMaster("local[*]")
    val spark = SparkSession.builder.config(sparkConf)
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY") // or "CORRECTED"
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    var read_file = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .option("delimiter", ",")
      .load("dataFiles/bankTransaction/bank_transactions.csv")

    read_file = read_file.withColumnRenamed(
      "TransactionAmount (INR)", "TransactionAmount")

    read_file.printSchema()
    read_file.persist()

    // Select specific columns
    val selected_columns = read_file.select(
      "TransactionID",
      "CustomerID",
      "CustGender",
      "CustAccountBalance",
      "TransactionDate",
      "TransactionAmount"
    )
    selected_columns.show()

    /**
     * +-------------+----------+
     * |TransactionID|CustomerID|
     * +-------------+----------+
     * |           T1|  C5841053|
     * |           T2|  C2142763|
     * |           T3|  C4417068|
     * +-------------+----------+
     */

    // Select using selectExpr
    val columns_with_expr = read_file.withColumn(
      "TransactionDate",
      date_format(from_unixtime(unix_timestamp(col("TransactionDate"), "MM/dd/yy")), "MM/dd/yyyy")
    ).select(
      "TransactionID",
      "CustomerID",
      "CustGender",
      "CustAccountBalance",
      "TransactionDate",
      "TransactionAmount"
    )

    columns_with_expr.show(3)

  }

}
