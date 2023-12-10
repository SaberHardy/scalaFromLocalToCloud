package com.exerices.transformations

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object crudOperations {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Transformations").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("Error")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val read_file = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .option("delimiter", ",")
      .load("dataFiles/bankTransaction/bank_transactions.csv")


    read_file.printSchema()

    read_file.persist()

    //    read_file.show(5)
    // Select specific columns

    val selected_columns = read_file.select(
      "TransactionID",
      "CustomerID",
      "CustGender",
      "CustAccountBalance",
      "TransactionDate",
      "TransactionAmount (INR)"
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


  }

}
