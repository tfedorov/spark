package com.tfedorov

import com.tfedorov.model.FreqPipelineBuilder
import org.apache.spark.internal.Logging
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.SQLTransformer
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
  private val testLocation = Option(args(1)).getOrElse("src\\main\\resources\\test.csv")
  private val trainLocation = Option(args(0)).getOrElse("src\\main\\resources\\train.csv")

  import sparkSession.implicits._

  val trainDF = sparkSession.read.format("csv")
    .option("header", "true") // Use first line of all files as header
    //.option("inferSchema", "true") // Automatically infer data types
    .load(trainLocation)
    .withColumn("label", 'label.cast(FloatType))
    .as[SentenceLabel]
  val testDF = sparkSession.read.format("csv")
    .option("header", "true") // Use first line of all files as header
    //.option("inferSchema", "true") // Automatically infer data types
    .load(testLocation)
    .withColumn("label", 'label.cast(FloatType))
    .as[SentenceLabel]

  val stopWords = Seq("від", "навіть", "про", "які", "до", "та", "як", "із", "що", "під", "на ", "не", "для", "за", "тому", "це")

  val pipeline: Pipeline = FreqPipelineBuilder(stopWords)
  val model = pipeline.fit(trainDF)

  val testResults = model.transform(testDF)

  testResults.show()

  val sqlTrans = new SQLTransformer().setStatement(
    "SELECT *, (label == prediction) AS Diff FROM __THIS__")

  sqlTrans.transform(testResults).show()
  sparkSession.close()
  log.trace("Finished")
}
