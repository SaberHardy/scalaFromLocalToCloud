package com.exerices.dataCollections

object GroupBy {
  def main(args: Array[String]): Unit = {
    val ages = List(20, 35, 21, 48, 22, 12)
    println(s"The ages are: \n $ages \n")

    val groups = ages.groupBy(age => if (age > 25) "Senior" else "Junior")

    println(groups)
    // Get the group of juniors
    println(groups("Junior"))
    println(groups("Senior"))

    // Group the ages into two groups:
    val two_groups = ages.grouped(2).foreach(group =>
      group.foreach(x => println(x * 2))
    )
    println(two_groups)
  }
}
