package com.tfedorov

import org.testng.Assert
import org.testng.annotations.Test

/**
  * Created by Taras_Fedorov on 10/2/2017.
  */
class SparkAppTest extends Assert {

  @Test
  def testApp(): Unit = {
    SparkApp.main(Array(""))
  }

}
