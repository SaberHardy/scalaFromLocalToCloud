package com.exerices.RDDs

import org.apache.spark.sql._
import org.apache.spark.api.java.JavaRDD.fromRDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDTransformations {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("BigBasketJob")
      .setMaster("local[*]")

    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")

    val data = sparkContext.textFile("dataFiles/data.txt")

    // Upper case all the values
    data.map(x=>x.toUpperCase()).foreach(println)

    // Flat map
    println("**********Flat map*************")
    data.take(3).flatMap(x=> x.split(",")).foreach(println)

    println("======== Filter English records =============")
//    data.foreach(println)
    val english_data = data.filter(x => x.contains("Engineer"))
    english_data.foreach(println)

    val entrepreneur_data = data.filter(x => x.contains("Entrepreneur"))
    entrepreneur_data.foreach(println)

    println("======== Filter intersection records =============")

    val intersection_data = english_data.intersection(english_data)
    intersection_data.foreach(println)
  }
}
