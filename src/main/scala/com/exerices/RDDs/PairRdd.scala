package com.exerices.RDDs

import org.apache.spark.{SparkConf, SparkContext}

object PairRdd {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("PairRdd").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("Error")

    val file_read = sparkContext.textFile("dataFiles/sales.txt")
    //    Create a pair RDD
    val pairRdd = file_read.map(x => (x.split(" ")(0), x.split(" ")(1).toInt))

    val collection = pairRdd.collect()

    for (item <- collection) {
      println(item._2)
    }

    /*Group By*/

    val salesGroupBy = pairRdd.groupByKey().collect()
    for (i <- salesGroupBy) {
      println(i)
    }

    /* Reduce by Key */
    println("******** Reduce By Key ********")
    val reduceByKey = pairRdd.reduceByKey(_ + _).collect()
    for (i <- reduceByKey) {
      println(i)
    }
  }
}
