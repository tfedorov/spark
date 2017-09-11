package com.tfedorov

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Created by Taras_Fedorov on 9/8/2017.
  */
object SparkApp extends App {

  println(s"Application ${this.getClass.getCanonicalName}")

  val sparkSession: SparkSession = SparkSession.builder.
    master("local")
    .appName("spark test")
    .getOrCreate()
  val sc: SparkContext = sparkSession.sparkContext
  val rdd1 = sc.parallelize(Seq(1, 2, 3))
  println(rdd1.reduce(_ + _))
}
