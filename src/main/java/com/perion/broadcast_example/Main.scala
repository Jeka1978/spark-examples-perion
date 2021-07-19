package com.perion.broadcast_example

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Evgeny Borisov
 */
object Main {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("taxi")
    val sc = new SparkContext(conf)

    GarbageWords.broadCastList(sc)

    val lines = sc.textFile("data/beatles.txt")
    val wordsService = new WordsService
    val strings = wordsService.topX(lines, 3)
    strings.foreach(println(_))
  }
}
