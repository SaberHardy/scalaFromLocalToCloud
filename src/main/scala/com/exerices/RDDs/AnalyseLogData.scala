package com.exerices.RDDs

import org.apache.spark.{SparkConf, SparkContext}

object AnalyseLogData {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("Log Data")
      .setMaster("local[*]")

    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")

    val all_rdd = sparkContext.textFile("dataFiles/logData.log")
    //    all_rdd.foreach(println)
    val info_rdds = all_rdd.filter(data => data.contains("INFO"))
    // Print the data logs that has just the "info" string
    info_rdds.take(3).foreach(println)
    println(s"INFO: ${info_rdds.count()}")

    val warning_rdds = all_rdd.filter(data => data.contains("WARN"))
    // Print the data logs that has just the "warn" string
    warning_rdds.take(3).foreach(println)
    println(s"WARN: ${warning_rdds.count()}")

    /** Combine two RDDS */
    val combined_data = info_rdds.union(warning_rdds)

    println(s"All The union data: ${combined_data.count()}") // 1848

    // Using collect data
    val union_data = combined_data.collect()
    println(union_data(2))

  }
}
