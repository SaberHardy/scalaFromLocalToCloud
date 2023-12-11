package com.exerices.transformations

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
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

    file_content.persist()

    val counted_data = file_content.count()
    println(s"All the data are: $counted_data")
    file_content.printSchema()

    val removed_duplicates = file_content.distinct().count()

    println(s"All the data are: $removed_duplicates")

    // Drop Duplication in columns
    println("_________Drop Duplication from rows_______")

    val dropped_duplication = file_content.distinct().dropDuplicates("CustomerID")
    println(dropped_duplication.count())

    println("******** Sort the Data *************")

    val sorted_data = dropped_duplication.orderBy(col("CustGender").desc) // or asc by default
    val null_first = dropped_duplication.orderBy(col("CustGender").asc_nulls_first) // or asc by default
    sorted_data.show()
    null_first.show()

    println("******** duplication > 1 *************")

    val duplicates = dropped_duplication.groupBy("CustGender").count().filter("count > 1")
    duplicates.show()

    spark.stop()
  }
}
