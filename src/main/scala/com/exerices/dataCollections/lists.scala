package com.exerices.dataCollections

object lists {
  """
    |Accessing Elements:
    |
    |apply(index: Int): Returns the element at the specified index.
    |head: Returns the first element.
    |tail: Returns a list containing all elements except the first one.
    |Adding Elements:
    |
    |:: or +:: Prepends an element to the list.
    |:+: Appends an element to the list.
    |Concatenation:
    |
    |++: Concatenates two lists.
    |Filtering and Transformation:
    |
    |filter: Returns a new list with elements satisfying a given predicate.
    |map: Applies a function to each element, returning a new list.
    |Sorting and Reversing:
    |
    |sorted: Returns a new list with elements in sorted order.
    |reverse: Returns a new list with elements in reverse order.
    |Aggregation:
    |
    |sum, product: Computes the sum or product of the elements.
    |max, min: Finds the maximum or minimum element.
    |Grouping and Partitioning:
    |
    |groupBy: Groups elements based on a key function.
    |partition: Partitions elements into two lists based on a predicate.
    |Folding and Reducing:
    |
    |foldLeft, foldRight: Applies a binary operator to the elements.
    |reduceLeft, reduceRight: Reduces the elements using a binary operator.
    |Element Existence:
    |
    |contains: Checks if a specific element is present in the list.
    |exists: Checks if any element satisfies a given predicate.
    |forall: Checks if all elements satisfy a given predicate.
    |List-specific Operations:
    |
    |init: Returns all elements except the last one.
    |last: Returns the last element.
    |take(n): Returns the first n elements.
    |drop(n): Returns all elements except the first n.
    |splitAt(n): Splits the list at the specified index.
    |Conversion:
    |
    |toArray, toSeq, toVector, toSet: Converts the list to other collection types.
    |String Representation:
    |
    |mkString: Converts the list elements to a string.
    |""".stripMargin

  def main(args: Array[String]): Unit = {
    // Append two lists
    val list1 = List(1, 2, 3, 4, 5)
    val list2 = List(6, 7, 8, 9, 10)
    // This is not the most better way to add two lists
    val list3 = list1 :+ list2

    //    the better way is
    var list4 = list1 ::: list2
    println(list3)
    println(list4)
  }
}
