package com.exerices.WorkWithJSON

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.StructType

import java.lang.Character.getType
import scala.io.Source

object ReadFromAPI {
  private def flatten_data_api(schema: StructType, prefix: String = null): Array[Column] = {
    schema.fields.flatMap(
      f => {
        val columnName = if (prefix == null) f.name else prefix + "." + f.name
        println(s"Column name $columnName")
        f.dataType match {
          case structType: StructType => flatten_data_api(structType, columnName)
          case _ => Array(col(columnName).as(columnName.replace(".", "_")))
        }
      }
    )
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("MultiLines")
      .setMaster("local[*]")

    val spark = SparkSession.builder.config(sparkConf)
      // .config("spark.sql.legacy.timeParserPolicy", "LEGACY") // or "CORRECTED"
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val apiUrl = "https://randomuser.me/api/"
    val input_data = Source.fromURL(apiUrl).mkString

    val rdd = spark.sparkContext.parallelize(List(input_data))
    val df = spark.read.json(rdd)

    df.printSchema()
    //    df.show()

    println("---- Flatten the data ----")

    val flatten_data = df.withColumn(
      "results", explode(col("results")))

    flatten_data.printSchema()
    flatten_data.show(false)

    //    val selected_data = flatten_data.select(
    //      col("info.seed"),
    //      col("info.version"),
    //      col("results.cell"),
    //      col("results.dob.age"),
    //      col("results.dob.date"),
    //      col("results.gender"),
    //      col("results.location.city"),
    //      col("results.location.state"),
    //      col("results.name.first"),
    //    )

    //    selected_data.show()
    //    selected_data.printSchema()

    //    flatten_data_api(flatten_data.schema)

    // Select the columns using the schema

    val api_data_flatten = flatten_data.select(flatten_data_api(flatten_data.schema): _*)

    api_data_flatten.printSchema()
    api_data_flatten.show()

  }
}
