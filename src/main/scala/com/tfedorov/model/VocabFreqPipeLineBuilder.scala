package com.tfedorov.model

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.VocabFreqTransformer

/**
  * Created by Taras_Fedorov on 4/14/2017.
  */
object VocabFreqPipeLineBuilder {

  def apply(stopWords: Seq[String]): Pipeline = {

    val regexTokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("tokens")
      .setPattern("""[ ,.!?№()-/—\\"_$]""") // alternatively .setPattern("\\w+").setGaps(false)

    val vocabFreq = new VocabFreqTransformer(stopWords)


    val mlr = new LogisticRegression().setMaxIter(1000).setTol(0.00001).setFamily("multinomial")

    val pipeline = new Pipeline().setStages(Array(regexTokenizer, vocabFreq, mlr))
    pipeline
  }

}
