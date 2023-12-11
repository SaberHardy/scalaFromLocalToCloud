package com.exerices.StringFunctions

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws, instr}

object StrFunctions {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Remove Null").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)

    sparkContext.setLogLevel("Error")

    val spark = SparkSession.builder().getOrCreate()

    val file_content = spark.read.format("csv")
      .option("header", true)
      .load("dataFiles/bankTransaction/bank_accounts.csv")

    file_content.persist()

    file_content.show()

    file_content.withColumn("UniqueID",
      concat_ws("-", col("TransactionID"), col("CustomerID")))
      .withColumn("LowerData",
        instr(col("CustLocation"), "A"))


    println("_________ This with new cols ___________")
    file_content.show()
  }
}
