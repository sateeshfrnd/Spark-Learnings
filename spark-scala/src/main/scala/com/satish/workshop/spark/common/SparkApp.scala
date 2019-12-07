package com.satish.workshop.spark.common

import org.apache.spark.sql.SparkSession

trait SparkApp {
  val runningOnCluster = false

  lazy val spark : SparkSession = {
    SparkSession.builder().master(master = "local").appName("Spark App").enableHiveSupport().getOrCreate()
  }

  val sparkContext = spark.sparkContext
}
