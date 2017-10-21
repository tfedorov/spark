package com.tfedorov.model

object VocabFreq {

  def apply(input: Seq[String], vocabulary: Set[String]): Seq[Double] = {

    val tokenNum = input.length

    val onlyVocabWords = input.filter(vocabulary.contains(_))
    val mapTokenCount = onlyVocabWords.groupBy(t => t).map(kv => (kv._1, kv._2.length))

    vocabulary.toSeq.map(mapTokenCount.getOrElse(_, 0)).map(1.0 * _ / onlyVocabWords.size)
  }
}
