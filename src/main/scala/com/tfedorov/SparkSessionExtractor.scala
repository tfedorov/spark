package com.tfedorov

import org.apache.spark.sql.SparkSession

/**
  * Created by Taras_Fedorov on 9/11/2017.
  */
protected object SparkSessionExtractor {

  var maybeSparkSession: Option[SparkSession] = None

  def apply(): SparkSession = maybeSparkSession.getOrElse(buildSession())

  private def buildSession(): SparkSession = SparkSession.builder.
    master("local")
    .appName("spark test")
    .getOrCreate()


}
