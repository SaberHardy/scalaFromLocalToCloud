package com.exerices.RDDs

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

object rowRDDs {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("BigBasketJob")
      .setMaster("local[*]")

    val sparkContext = new SparkContext(sparkConf)

    sparkContext.setLogLevel("ERROR")

    val read_file = sparkContext.textFile("dataFiles/countries.txt")
    val split_data = read_file.map(x => x.split(","))
    val input_rows = split_data.map(x => Row(x(0), x(1), x(2), x(3)))

    val filtered_english = input_rows.filter(x => x(2).toString.contains("English"))

    filtered_english.coalesce(1).saveAsTextFile("dataFiles/RowRDDs")

    filtered_english.foreach(println)
  }
}
