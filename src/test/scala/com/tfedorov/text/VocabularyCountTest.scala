package com.tfedorov.text

import org.testng.Assert._
import org.testng.annotations.Test

class VocabularyCountTest {

  @Test
  def testArticlesApp(): Unit = {
    val actual = VocabFreq(Seq("а", "б", "а", "в"), Set("а", "в"))
    assertEquals(actual, Seq(0.5, 0.25))


    val res = VocabFreq(Seq("а", "б", "а", "в"), Set("а", "в", "г"))
    assertEquals(res, Seq(0.5, 0.25, 0))
  }

}
