package com.tfedorov

import org.testng.Assert
import org.testng.annotations.Test

/**
  * Created by Taras_Fedorov on 10/2/2017.
  */
class SparkAppTest extends Assert {

  @Test
  def testArticlesApp(): Unit = {
    SparkApp.main(Array("src\\test\\resources\\train.csv","src\\test\\resources\\test.csv"))
  }

  @Test
  def testLitratureApp(): Unit = {
    //SparkApp.main(Array("src/test/resources/litreture_train.csv", "src/test/resources/litreture_test.csv"))
  }

}
