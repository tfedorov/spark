package com.tfedorov

import com.tfedorov.model.FreqPipelineBuilder
import org.apache.spark.internal.Logging
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{RegexTokenizer, SQLTransformer}
import org.apache.spark.ml.linalg.{Vectors, VocabFreqTransformer}
import org.apache.spark.sql
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

  val regexTokenizer = new RegexTokenizer()
    .setInputCol("sentence")
    .setOutputCol("tokens")
    .setPattern("""[ ,.!?№()-/—\\"_$]""") // alternatively .setPattern("\\w+").setGaps(false)
  val tokened = regexTokenizer.transform(trainDF)

  val listWordsFreq = new VocabFreqTransformer(stopWords)

  val freqDF = listWordsFreq.transform(tokened)

  val metadata = new sql.types.MetadataBuilder().putLong("numFeatures", 8).build()

  val newColumn = freqDF.col("features").as("features2", metadata)
  val f = freqDF.withColumn("features2", newColumn)

  val mlr = new LogisticRegression()
    .setMaxIter(100)
    .setRegParam(0.01)
    .setFeaturesCol("features2")
    .setElasticNetParam(0.1)
    .setFamily("multinomial")

  val traningDataSet = sparkSession.createDataFrame(Seq(
    (1.0, Vectors.dense(0.0, 1.1, 0.1, 0.0, 1.1, -0.1, 0.0, 1.1, 0.1, 0.0, 1.1, 0.1, 0.2, 1.1, 0.1, 0.2)),
    (0.0, Vectors.dense(2.0, 1.0, -1.0, 1.1, 0.1, 0.0, 1.1, 0.1, 0.0, 1.1, 0.1, 0.2, 0.2, 1.1, 0.1, 0.2)),
    (0.0, Vectors.dense(2.0, 1.3, 1.0, 1.1, -0.1, 0.0, 1.1, 0.1, 0.0, 1.1, 0.1, 0.2, 0.2, 1.1, 0.1, 0.2)),
    (1.0, Vectors.dense(0.0, 1.2, -0.5, 1.1, 0.1, 0.0, 1.1, 0.1, 0.0, 1.1, 0.1, 0.2, 0.2, 1.1, 0.1, 0.2))
  )).toDF("label", "features")
  val logisticRegression = new LogisticRegression()
  val logisticRegressionModel = logisticRegression.fit(traningDataSet)
  logisticRegressionModel.transform(traningDataSet).show
  logisticRegressionModel.transform(freqDF).show

 // mlr.fit(f).transform(f).show

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
