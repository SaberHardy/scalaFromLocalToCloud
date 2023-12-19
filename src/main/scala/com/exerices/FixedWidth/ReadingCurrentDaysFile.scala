package com.exerices.FixedWidth

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import java.time.LocalDate

object ReadingCurrentDaysFile {

  def splitDataWithColumns(columnData: Array[Row],
                           new_column_name: String): List[String] = {

    var final_dml: List[String] = Nil

    for (i <- columnData) {
      val data = i.mkString(",")

      val columnName = data.split(",")(0)
      val pos = data.split(",")(1)
      val len = data.split(",")(2)

      val final_dm = s"substring($new_column_name, $pos, $len) as $columnName"

      final_dml = final_dml :+ final_dm

    }
    final_dml
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("Read current days")
      .setMaster("local[*]")

    val sparkContext = new SparkContext(sparkConf)
    val spark = SparkSession.builder().getOrCreate()

    sparkContext.setLogLevel("Error")

    // Read the file
    val schemaFile = spark.read
      .format("json")
      .load("dataFiles/FixedWidthFiles/dml.json")
    schemaFile.persist()

    val collected_data = schemaFile.collect()
    val all_the_collected_data = splitDataWithColumns(
      collected_data, "fixed_data")

    val inputFile = spark.read
      .format("text")
      .option("header", false)
      .load("dataFiles/FixedWidthFiles/fixedData.txt")
      .withColumnRenamed("value", "fixed_data")

    //      .withColumnRenamed("_c0", "fixed_data")
    println(s"input file: $inputFile")
    inputFile.persist()

    //    println(all_the_collected_data)

    val df = inputFile.selectExpr(all_the_collected_data: _*)
    df.show(false)

  }
}
