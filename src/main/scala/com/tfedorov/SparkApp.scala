package com.tfedorov

import org.apache.spark.internal.Logging

/**
  * Created by Taras_Fedorov on 9/8/2017.
  */
object SparkApp extends App with Logging {

  log.trace("Started")

  private val sparkSession = SparkSessionExtractor()
  private val sc = sparkSession.sparkContext

  private val result = sc.parallelize(Seq(1, 2, 3)).reduce(_ + _)

  log.trace(result.toString)

  sparkSession.close()
  log.trace("Finished")
}
