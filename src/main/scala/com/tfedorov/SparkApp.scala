package com.tfedorov

import com.tfedorov.model.FreqPipelineBuilder
import org.apache.spark.internal.Logging
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.types.FloatType


case class RawText(id: Int, sentence: String)

/**
  * Created by Taras_Fedorov on 9/8/2017.
  */
object SparkApp extends App with Logging {

  case class SentenceLabel(sentence: String, label: Float)

  log.trace("Started")

  private val sparkSession = SparkSessionExtractor()
  private val sc = sparkSession.sparkContext
  private val testLocation = Option(args(0)).getOrElse("src\\main\\resources\\test.csv")

  import sparkSession.implicits._

  private val trainDF = sparkSession.read.format("csv")
    .option("header", "true") // Use first line of all files as header
    //.option("inferSchema", "true") // Automatically infer data types
    .load(testLocation)
    .withColumn("label", 'label.cast(FloatType))
    .as[SentenceLabel]

  trainDF.show()
  /*
    private val trainRDD = sc.parallelize(Seq(
      SentenceLabel(DATA.TRAIN_1, 1.0f),
      SentenceLabel(DATA.TRAIN_4, 4.0f)))
*/

  import sparkSession.implicits._

  val pipeline: Pipeline = FreqPipelineBuilder()

  val model = pipeline.fit(trainDF)

  private val testRDD = sc.parallelize(Seq(
    SentenceLabel(DATA.TRAIN_1, 1.0f),
    SentenceLabel(DATA.TRAIN_4, 4.0f)))

  val testResults = model.transform(testRDD.toDS())

  testResults.show()


  sparkSession.close()
  log.trace("Finished")
}
