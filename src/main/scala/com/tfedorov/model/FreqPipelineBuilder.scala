package com.tfedorov.model

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.VocabFreqTransformer

/**
  * Created by Taras_Fedorov on 4/14/2017.
  */
object FreqPipelineBuilder {

  def apply(stopWords: Seq[String]): Pipeline = {

    val regexTokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("tokens")
      .setPattern("""[ ,.!?№()-/—\\"_$]""") // alternatively .setPattern("\\w+").setGaps(false)

    val listWordsFreq = new VocabFreqTransformer(stopWords)

    val mlr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.01)
      .setFeaturesCol("features")
      .setElasticNetParam(0.1)
      .setFamily("multinomial")

    val pipeline = new Pipeline().setStages(Array(regexTokenizer, listWordsFreq, mlr))
    pipeline
  }

}
