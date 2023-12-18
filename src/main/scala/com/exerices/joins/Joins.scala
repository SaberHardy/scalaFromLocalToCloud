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
    val inner_join_dfs = employee_df.join(depart_df,
      employee_df("DEPARTMENT_ID") === depart_df("Dno"), "inner")

    inner_join_dfs.show()

    println("_______ Left JOIN ________")
    val left_join_dfs = employee_df.join(depart_df,
      employee_df("DEPARTMENT_ID") === depart_df("Dno"), "left")

    left_join_dfs.show()

    println("_______ Right JOIN ________")
    val right_join_dfs = employee_df.join(depart_df,
      employee_df("DEPARTMENT_ID") === depart_df("Dno"), "right")

    right_join_dfs.show()

    println("_______ Full outer JOIN ________")
    val outer_join_dfs = employee_df.join(depart_df,
      employee_df("DEPARTMENT_ID") === depart_df("Dno"), "outer")

    outer_join_dfs.show()

    println("_______ Left Anti JOIN ________")
    val leftAnti_join_dfs = employee_df.join(depart_df,
      employee_df("DEPARTMENT_ID") === depart_df("Dno"), "left_anto")

    leftAnti_join_dfs.show()

    println("_______ Left Semi JOIN ________")
    val leftSemi_join_dfs = employee_df.join(depart_df,
      employee_df("DEPARTMENT_ID") === depart_df("Dno"), "left_semi")

    leftSemi_join_dfs.show()


  }

}
