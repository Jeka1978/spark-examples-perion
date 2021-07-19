package com.perion.broadcast_example

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

/**
 * @author Evgeny Borisov
 */
object GarbageWords {
  var broadcastedGarbage: Broadcast[List[String]]=null

  def broadCastList(sparkContext: SparkContext):Unit={
    broadcastedGarbage = sparkContext.broadcast(garbage())
  }

   def garbage():List[String]={
    List("i","to")
  }



}
