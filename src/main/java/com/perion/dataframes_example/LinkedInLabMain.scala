package com.perion.dataframes_example

import com.perion.taxi_lab.Trip
import org.apache.log4j.{Level, Logger}
import org.apache.spark.network.protocol.Encoders
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

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


    val dfWithSalary = dataFrame.withColumn("salary", col("age") * 10 * size(col("keywords")))

    val mostPopular: String = keywords.groupBy(col("keyword"))
      .agg(count(col("keyword")).as("amount"))
      .orderBy(col("amount").desc).first().getAs("keyword")

    dfWithSalary.filter(array_contains(col("keywords"),mostPopular))
      .filter(col("salary")<1200)
      .show()


//    dataFrame.join(broadcast(dfWithSalary),dataFrame("id")===dfWithSalary("my_id"))



   /* import org.apache.spark.sql.Encoders
    val encoder = Encoders.product[Trip]
    //spark.sql.jsonGenerator.ignoreNullFields

    spark.read.schema(encoder.schema).json()*/

  }

}
