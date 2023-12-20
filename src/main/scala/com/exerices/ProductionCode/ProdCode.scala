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
  }
}
