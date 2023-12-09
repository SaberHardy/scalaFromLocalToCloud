import org.apache.spark.sql.{SaveMode, SparkSession}

object ReadAvroData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Read Avro Data")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")  // Change the warehouse directory as needed
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val read_file = spark.read.format("text")
      .option("header", "true")
      .option("delimiter", ",")
      .load("dataFiles/countries.txt")

    read_file.write.format("com.databricks.spark.avro")
      .mode(SaveMode.Overwrite)
      .save("D:\\ScalaProjects\\ScalaExos\\jarData")

    spark.stop()
  }
}
