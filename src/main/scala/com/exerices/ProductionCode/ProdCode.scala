package com.exerices.ProductionCode

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ProdCode {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("Read current days")
      .setMaster("local[*]")

    val sparkContext = new SparkContext(sparkConf)
    val spark = SparkSession.builder().getOrCreate()
    sparkContext.setLogLevel("Error")

    val employee_data = spark.read.format("csv")
      .option("header", true)
      .load("dataFiles/joins/employees.csv")

    employee_data.show(false)

    employee_data.write.format("parquet").option("header", "true")
      .mode("overwrite")
      .save("dataFiles/joins/parquetData")

    println("the parquet data has been saved!!")

  }
}
