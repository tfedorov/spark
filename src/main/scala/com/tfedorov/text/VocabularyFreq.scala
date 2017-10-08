package com.tfedorov.text

object VocabularyFreq {

  def apply(input: Seq[String], vocabulary: Set[String]): Seq[Double] = {

    val tokenNum = input.length

    val mapTokenCount = input.filter(vocabulary.contains(_)).groupBy(t => t).map(kv => (kv._1, kv._2.length))

    vocabulary.toSeq.map(mapTokenCount.getOrElse(_, 0)).map(1.0 * _ / tokenNum)
  }
}
