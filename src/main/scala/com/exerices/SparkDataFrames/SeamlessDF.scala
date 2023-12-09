package com.exerices.SparkDataFrames

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SeamlessDF {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Seamless Dataframes").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("Error")

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val read_csv_file = sparkSession.read.format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .load("dataFiles/train.csv")

    read_csv_file.printSchema()
    print("***********Data Frame Content ****************")
    read_csv_file.show(5)

    print("*********** Take 5 rows ****************\n")

    //    val rea = read_csv_file.limit(5)
    //    five_rows.show()
    // Write into csv file
    read_csv_file.write
      // The format can be: csv, orc, parquet, Avro, json, txt, xls, xlsx...
      .format("csv")
      .option("header", "true")
      .option("delimiter", "|")
      .save("dataFiles/takeFiverows.csv")

    sparkSession.stop()

  }
}
