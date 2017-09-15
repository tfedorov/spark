package com.tfedorov

import com.tfedorov.model.FreqPipelineBuilder
import org.apache.spark.internal.Logging
import org.apache.spark.ml.Pipeline

/**
  * Created by Taras_Fedorov on 9/8/2017.
  */
object SparkApp extends App with Logging {

  case class SentenceLabel(sentence: String, label: Float)

  log.trace("Started")

  private val sparkSession = SparkSessionExtractor()
  private val sc = sparkSession.sparkContext

  private val trainRDD = sc.parallelize(Seq(
    SentenceLabel(DATA.TRAIN_1, 1.0f),
    SentenceLabel(DATA.TRAIN_4, 4.0f)))

  import sparkSession.implicits._

  val pipeline: Pipeline = FreqPipelineBuilder()

  val model = pipeline.fit(trainRDD.toDS())

  private val testRDD = sc.parallelize(Seq(
    SentenceLabel(DATA.TRAIN_1, 1.0f),
    SentenceLabel(DATA.TRAIN_4, 4.0f)))

  val testResults = model.transform(testRDD.toDS())

  testResults.show()

  sparkSession.close()
  log.trace("Finished")
}
