package com.exerices.transformations

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank, rank, row_number}

object WindowFunction {
  def main(args: Array[String]): Unit = {
    """
      | Window functions: are typically used in Spark SQL
      |  or DataFrame API for analytical queries.
      | Some common use cases for window functions include:
      | calculating running totals, moving averages, rank,
      | and other aggregations over a specified window of rows.
      |
      | RANK() may leave gaps in the ranking sequence when there
      |  are ties, while DENSE_RANK() does not skip ranks for tied
      |  values, resulting in a more compact or "dense" ranking.
      |  The choice between them depends on the specific
      |  requirements of your analysis and the desired behavior
      |  when there are ties in the order column.
      |""".stripMargin

    val sparkConf = new SparkConf().setAppName("Apply Aggregations").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    val spark = SparkSession.builder().getOrCreate()

    // Read the file
    val file_content = spark.read
      .format("csv")
      .option("header", true)
      .load("dataFiles/bankTransaction/bank_transactions.csv")
    file_content.persist()

    file_content.printSchema()

    file_content.show()

    val windowData = Window.partitionBy("CustomerID")
      .orderBy("CustAccountBalance")

    println("This is window function......!")

    file_content.select(
        col("CustomerID"),
        col("CustAccountBalance").cast("Double"))
      .withColumn("RowNumber", row_number().over(windowData))
      .withColumn("Rank", rank().over(windowData))
      .withColumn("DenseRank", dense_rank().over(windowData))
      .where(col("DenseRank") =!= col("rank"))
      .where(col("CustomerID") === "C1010024")
      .show()


    file_content.unpersist()
    spark.stop()

  }
}