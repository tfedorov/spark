package com.tfedorov

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
  * Created by Taras_Fedorov on 9/8/2017.
  */
object SparkApp extends App with Logging {

  log.trace("Started")

  val sparkSession: SparkSession = SparkSession.builder.
    master("local")
    .appName("spark test")
    .getOrCreate()
  val sc: SparkContext = sparkSession.sparkContext

  private val result: Int = sc.parallelize(Seq(1, 2, 3)).reduce(_ + _)
  log.trace(result.toString)

  sparkSession.close()
  log.trace("Finished")
}
