package com.tfedorov

import com.tfedorov.model.VocabFreqPipeLineBuilder
import org.apache.spark.internal.Logging
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.SQLTransformer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.FloatType

case class RawText(id: Int, sentence: String)

/**
  * Created by Taras_Fedorov on 9/8/2017.
  */
object SparkApp extends App with Logging {

  log.trace("Started")

  private val sparkSession = SparkSessionExtractor()
  private val sc = sparkSession.sparkContext
  private val testLocation = Option(args(1)).get
  private val trainLocation = Option(args(0)).get


  import sparkSession.implicits._

  val trainDF = sparkSession.read.format("csv")
    .option("header", "true") // Use first line of all files as header
    //.option("inferSchema", "true") // Automatically infer data types
    .load(trainLocation)
    .withColumn("label", 'label.cast(FloatType))

  val testDF = sparkSession.read.format("csv")
    .option("header", "true") // Use first line of all files as header
    //.option("inferSchema", "true") // Automatically infer data types
    .load(testLocation)
    .withColumn("label", 'label.cast(FloatType))

  val stopWords = Seq("що", "тому", "про", "які", "до", "та", "як", "із", "що", "під", "на", "не", "для", "за", "тому", "це")

  //  val stopWords = Seq("ся", "і", "від", "навіть", "про", "які", "до", "та", "як", "із", "що", "під", "на ", "не", "для", "за", "тому", "це")

  val pipeline: Pipeline = VocabFreqPipeLineBuilder(stopWords)
  val model = pipeline.fit(trainDF)

  val trainResults = model.transform(trainDF)
  val testResults = model.transform(testDF)


  val sqlTrans = new SQLTransformer().setStatement(
    "SELECT *, (label == prediction) AS Diff FROM __THIS__")

  private val resTrain: DataFrame = sqlTrans.transform(trainResults)
  resTrain.show()
  resTrain.select("features").foreach(rdd => println(rdd.get(0)))
  private val resTest: DataFrame = sqlTrans.transform(testResults)
  resTest.show()
  resTest.select("features").foreach(rdd => println(rdd.get(0)))
  sparkSession.close()
  log.trace("Finished")
}
