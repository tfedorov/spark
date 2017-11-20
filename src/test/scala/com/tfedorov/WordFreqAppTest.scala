package com.tfedorov

import org.testng.Assert
import org.testng.annotations.Test

/**
  * Created by Taras_Fedorov on 10/2/2017.
  */
class WordFreqAppTest extends Assert {

  @Test
  def testArticlesApp(): Unit = {
    WordFreqApp.main(Array("src/test/resources/train.csv", "src/test/resources/test.csv"))
  }

  @Test
  def testLiteratureApp(): Unit = {
    WordFreqApp.main(Array("src/test/resources/literature_train.csv", "src/test/resources/literature_test.csv"))
  }

}
