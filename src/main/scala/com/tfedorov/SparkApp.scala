package com.tfedorov

import com.tfedorov.text.VocabularyFreq
import org.apache.spark.internal.Logging
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.{col, udf}
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
  //  private val testLocation = Option(args(1)).getOrElse("src\\main\\resources\\test.csv")
  private val trainLocation = Option(args(0)).getOrElse("src\\main\\resources\\train.csv")

  import sparkSession.implicits._

  val trainDF = sparkSession.read.format("csv")
    .option("header", "true") // Use first line of all files as header
    //.option("inferSchema", "true") // Automatically infer data types
    .load(trainLocation)
    .withColumn("label", 'label.cast(FloatType))
    .as[SentenceLabel]

  val stopWords = Seq("від", "навіть", "про", "які", "до", "та", "як", "із", "що", "під", "на ", "не", "для", "за", "тому", "це")

  val regexTokenizer = new RegexTokenizer()
    .setInputCol("sentence")
    .setOutputCol("allText")
    .setPattern("""[ ,.!?№()-/—\\"_$]""") // alternatively .setPattern("\\w+").setGaps(false)

  val tokenedDF = regexTokenizer.transform(trainDF)
  tokenedDF.show()
  val changedUDF = udf { document: Seq[String] => Vectors.dense(VocabularyFreq(document, stopWords.toSet).toArray) }

  val freqWordDF = tokenedDF.withColumn("features", changedUDF(col("allText")))

  freqWordDF.show()

  val mlr = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.01)
    .setFeaturesCol("features")
    .setElasticNetParam(0.1)
    .setFamily("multinomial")

  mlr.fit(freqWordDF)

  /*
    val interaction = new Interaction()
      .setInputCols(Array("oneWordFreq", "wordFreq"))
      .setOutputCol("features")

    val res = interaction.transform(freqWordDF)

    res.show()
    */
  /*
   val pipeline: Pipeline = FreqPipelineBuilder(stopWords)

   val model = pipeline.fit(trainDF)

   private val testDF = sparkSession.read.format("csv")
     .option("header", "true") // Use first line of all files as header
     //.option("inferSchema", "true") // Automatically infer data types
     .load(testLocation)
     .withColumn("label", 'label.cast(FloatType))
     .as[SentenceLabel]

   val testResults = model.transform(testDF)

   testResults.show()
   val res = testResults.select("allText").rdd.take(10).map(_.get(0))
   res.foreach(print)
 */
  sparkSession.close()
  log.trace("Finished")
}
