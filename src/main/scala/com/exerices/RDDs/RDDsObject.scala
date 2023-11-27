package com.exerices.RDDs

import org.apache.spark.{SparkConf, SparkContext}

object RDDsObject {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("BigBasketJob")
      .setMaster("local[*]")

    val sparkContext = new SparkContext(sparkConf)

    sparkContext.setLogLevel("ERROR")

    try {
      val data = sparkContext.textFile("dataFiles/BigBasketProducts.csv")

      // Filter Data
      val filter_category = data.filter(line => line.contains("Beauty"))

      // Show the filtered data
      //      filter_category.take(5).foreach(println)

      // Save the filtered data to a single file
      filter_category.coalesce(2).saveAsTextFile("dataFiles/coalesce")
    } finally {
      sparkContext.stop()
    }
  }
}
