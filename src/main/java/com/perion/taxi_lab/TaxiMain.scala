package com.perion.taxi_lab

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
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
    val linesRdd = sc.textFile("data/trips.txt")

    val tripsRdd: RDD[Trip] = linesRdd.map(_.split(" "))
      .map(arr => Trip(arr(0), arr(1).toLowerCase, arr(2).toInt))


    val rdd = tripsRdd.persist()

    val longTripsToBoston = tripsRdd.filter(_.city == "boston").filter(_.km > 10).count()

    println(s"long trips to boston: $longTripsToBoston")


    val totalKmToBoston = tripsRdd.filter(_.city == "boston").map(_.km).sum()

    println(s"total to boston: $totalKmToBoston")


    tripsRdd.map(trip=>(trip.id,trip.km))
      .reduceByKey(_+_)
      .map(_.swap)
      .sortByKey(ascending = false)
      .take(3)
      .foreach(println(_))









  }
}