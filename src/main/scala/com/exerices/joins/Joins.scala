package com.exerices.joins

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Joins {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Apply Joins").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    val spark = SparkSession.builder().getOrCreate()

    sparkContext.setLogLevel("Error")

    // Read the file
    val employee_df = spark.read
      .format("csv")
      .option("header", true)
      .load("dataFiles/joins/employees.csv")
    employee_df.printSchema()

    val depart_df = spark.read
      .format("csv")
      .option("header", true)
      .load("dataFiles/joins/department.csv")
    depart_df.printSchema()

    employee_df.persist()
    depart_df.persist()

    println("_______ Inner JOIN ________")
    val join_dfs = employee_df.join(depart_df,
      employee_df("DEPARTMENT_ID") === depart_df("Dno"), "inner")

    join_dfs.show()


  }

}
