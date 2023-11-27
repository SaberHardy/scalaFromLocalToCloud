package com.exerices.dataCollections


object Sequence {
  """
    |Accessing Elements:
    |
    |apply(index: Int): Returns the element at the specified index.
    |head: Returns the first element.
    |tail: Returns a sequence containing all elements except the first one.
    |Adding Elements:
    |
    |+: or ::: Prepends an element to the sequence.
    |:+: Appends an element to the sequence.
    |Concatenation:
    |
    |++: Concatenates two sequences.
    |Filtering and Transformation:
    |
    |filter: Returns a new sequence with elements satisfying a given predicate.
    |map: Applies a function to each element, returning a new sequence.
    |Sorting and Reversing:
    |
    |sorted: Returns a new sequence with elements in sorted order.
    |reverse: Returns a new sequence with elements in reverse order.
    |Aggregation:
    |
    |sum, product: Computes the sum or product of the elements.
    |max, min: Finds the maximum or minimum element.
    |Grouping and Partitioning:
    |
    |groupBy: Groups elements based on a key function.
    |partition: Partitions elements into two sequences based on a predicate.
    |Folding and Reducing:
    |
    |foldLeft, foldRight: Applies a binary operator to the elements.
    |reduceLeft, reduceRight: Reduces the elements using a binary operator.
    |Element Existence:
    |
    |contains: Checks if a specific element is present in the sequence.
    |exists: Checks if any element satisfies a given predicate.
    |forall: Checks if all elements satisfy a given predicate.
    |Conversion:
    |
    |toArray, toList, toVector, toSet: Converts the sequence to other collection types.
    |String Representation:
    |
    |mkString: Converts the sequence elements to a string.
    |""".stripMargin

  def main(args: Array[String]): Unit = {
    val sequence = Seq(1, 2, 5, 7, 4, 5)
    val result = sequence.map(x => x * 3).mkString(", ")

    println(result.mkString)

  }
}
