package com.exerices.transformations

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.{SparkConf, SparkContext}

object HandelNullValues {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Remove Null").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("Error")

    val spark = SparkSession.builder().getOrCreate()

    val file_content = spark.read.format("csv")
      .option("header", true)
      .load("dataFiles/bankTransaction/account.csv")

    file_content.persist()

    file_content.show()

    file_content.na.drop(Seq("bank_id")).show()
  }
}
