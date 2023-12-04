package com.exerices.RDDs

import org.apache.spark.{SparkConf, SparkContext}

object wordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Word Count").setMaster("local[*]")
    val spark = new SparkContext(sparkConf)
    spark.setLogLevel("Error")

    val read_file = spark.textFile("dataFiles/count_data_ex.txt")
    val word_count = read_file
                        .flatMap(s => s.split(" "))
                        .map(word => (word, 1))
                        .reduceByKey((x, y) => x + y)

    word_count.collect().sortBy(x=>x).foreach(println)
  }
}
