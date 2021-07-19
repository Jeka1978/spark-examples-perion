package com.perion.dataframes_example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

/**
 * @author Evgeny Borisov
 */
object LinkedInLabMain {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder().master("local[*]").appName("linkedIn").getOrCreate()
    val dataFrame: DataFrame = spark.read.json("data/profiles.json")
    dataFrame.show(truncate = false)
    dataFrame.printSchema()


//    dataFrame.withColumn("number of techologies",size(col("keywords"))).show()

    val keywords = dataFrame.withColumn("keyword", explode(col("keywords"))).select("keyword")

    keywords.createOrReplaceTempView("keywords")
    spark.sql("select keyword, count(*) as c from keywords group by keyword order by c desc").show()



    keywords.groupBy(col("keyword"))
      .agg(count(col("keyword")).as("amount"))
      .orderBy(col("amount").desc).show()












  }

}
