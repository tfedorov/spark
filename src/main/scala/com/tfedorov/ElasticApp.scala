package com.tfedorov

import org.apache.spark.internal.Logging
import org.elasticsearch.spark._

/**
  * Created by Taras_Fedorov on 9/11/2017.
  */
object ElasticApp extends App with Logging {

  log.trace("Started")
  val sparkSession = SparkSessionExtractor()
  val sc = sparkSession.sparkContext

  val conf = sparkSession.conf
  // Elastic connection parameters
  val elasticConf = Map(
    "es.nodes.ingest.only" -> "false",
    "es.nodes.data.only" -> "false")

  val airports = Map("arriva2" -> "Otopeni", "SFO" -> "San Fran")

  sc.makeRDD(Seq(airports)).saveToEs("tutorial/elastic", elasticConf)

  log.trace("finished")
}
