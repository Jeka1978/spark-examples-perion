package com.perion.broadcast_example

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Assert, Test}

/**
 * @author Evgeny Borisov
 */
class TestWordsService {


  @Test
  def testWordsService(): Unit = {

    val wordsService = new WordsService


    val conf = new SparkConf().setMaster("local[*]").setAppName("taxi")
    val rdd = new SparkContext(conf).parallelize(List("java", "java", "Java", "JAVA", "Scala", "scala", "C#", "python", "python", "python"))


    val strings = wordsService.topX(rdd, 2)
    strings.foreach(println(_))
    Assert.assertEquals(2,strings.length)
    Assert.assertEquals("java",strings(0))
    Assert.assertEquals("python",strings(1))

  }
}



