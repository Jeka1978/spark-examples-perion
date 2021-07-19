package com.perion.taxi_lab

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Evgeny Borisov
 */
object TaxiMain {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("taxi").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.textFile("data/trips.txt").collect().foreach(println(_))


  }
}