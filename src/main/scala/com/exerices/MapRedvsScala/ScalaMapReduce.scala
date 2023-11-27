package com.exerices.MapRedvsScala



object ScalaMapReduce {
  def main(args: Array[String]): Unit = {
    // RDD: Immutable collection of objects
    // Operations:
    // |==> Transformation: Map, filter, flatMap, union, distinct
    // |==>Actions: collect,count,reduce,take,foreach

    /**    MapReduce                   Vs       Spark
     * follow linear evaluation,              Spark Lazy evaluation
     * top to bottom approach,                Spark bottom to top (action to rdd)
     * there are frequent hits to hard disk,  in memory processing
     * */
  }
}
