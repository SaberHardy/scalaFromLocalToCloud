package com.exerices.dataCollections
//import scala.collection.immutable._

object sets {
  def main(args: Array[String]): Unit = {
    val set_numbers = Set(1, 2, 4, 3, 4, 4, 345)
    println(set_numbers)

    val set_strings = Set("Hello", "World", "I'm", "Algeria")
    println(set_strings)

    println(set_numbers.head) // This will return the first element
    println(set_numbers.tail) // Return all the elements except the first
    println(set_numbers.isEmpty) // Return True/False if empty
    println(set_numbers.size) // Return size of the set
    println(set_numbers.contains(3)) // True/False if the element exists

    println()
    println("iterate throw the sets")
    println()

    for (item <- set_strings) {
      print(item +" ")
    }
    println()
    //or we can do it like this

    set_numbers.foreach(x => print(x * 2 + " "))



  }

}
