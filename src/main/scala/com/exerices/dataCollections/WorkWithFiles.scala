package com.exerices.dataCollections

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}
import scala.io.Source

object WorkWithFiles {
  def main(args: Array[String]): Unit = {
    /** *
     * //    This is for the file already exists
     * // This line will return an iterator
     * val iterator_data = Source.fromFile("dataFiles/data.txt")
     * //    val data_file = iterator_data.mkString
     *
     * val get_lines = iterator_data.getLines()
     * //    get_lines.foreach(line => println(line + ", added"))
     * // Print line by line with slices
     * //    get_lines.slice(3, 6).foreach(x => println(x))
     *
     * // Convert every line to list
     * println(get_lines.toList(3))
     *
     * iterator_data.close()
     * * */
    // Second Part
    /**
     * This part for file creation
     *
     */
    //    val file_output = new PrintWriter(new File("dataFiles/EmployeeAges.txt"))
    //    for (i <- 1 to 10) {
    //      file_output.write(s"Line number $i\n")
    //    }

    // Append data to existing one
    val fileOutput = new PrintWriter(
      new BufferedWriter(
        new FileWriter(
          "dataFiles/EmployeeAges.txt",
          false)
      )
    )
    //
    for (j <- 1 to 3) {
      fileOutput.write(s"Hello from $j word\n")
    }

    fileOutput.close()
  }
}
