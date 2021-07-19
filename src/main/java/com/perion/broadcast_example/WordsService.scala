package com.perion.broadcast_example

import org.apache.spark.rdd.RDD

/**
 * @author Evgeny Borisov
 */
class WordsService {

  def topX(linesRdd: RDD[String], x: Int): Array[String] = {
    linesRdd.map(_.toLowerCase())
      .flatMap(_.split(" "))
      .filter(word => !GarbageWords.broadcastedGarbage.value.contains(word))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortBy(tuple => tuple._2, ascending = false)
      .map(tuple => tuple._1)
      .take(x)

  }

}
