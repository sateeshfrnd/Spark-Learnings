package com.satish.workshop.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger

/**
 * Usage:
 * Local:
 * spark-submit --class com.satish.workshop.spark.Wordcount --master local[*] spark-0.0.1-SNAPSHOT.jar '/user/ec2-user/satish/wc_input.txt' '/user/ec2-user/satish/wccount'
 * 
 * Cluster
 * spark-submit --class com.satish.workshop.spark.Wordcount --master spark://172.31.28.41:7077 spark-0.0.1-SNAPSHOT.jar '/user/ec2-user/satish/wc_input.txt' '/user/ec2-user/satish/wccount'
 */
object Wordcount {
  def main(arg: Array[String]) {

    var logger = Logger.getLogger(this.getClass())
    val JOB_NAME = "WordCount"

    if (arg.length < 2) {
      logger.error("=> wrong parameters number")
      println("Usage: Pass Arguments <InputFile-Path> <OutputFile-Path>")
      System.exit(1)
    }

    val inputPath = arg(0)
    val outputPath = arg(1)

    // Create SparkConf Instance
    val sparkConf = new SparkConf().setAppName(JOB_NAME)
    sparkConf.setMaster("local")

    // Create SparkContext
    val sparkContext = new SparkContext(sparkConf)

    logger.info("=> JOB_NAME \"" + JOB_NAME + "\"")
    logger.info("=> inputPath \"" + inputPath + "\"")

    val lines = sparkContext.textFile(inputPath)
    val numLines = lines.map(x => x.size)
    logger.info("=> numLines \"" + numLines + "\"")

    val wc = lines.flatMap { l => l.split(" ") }
      .map { word => (word, 1) }
      .reduceByKey(_ + _)

    wc.saveAsTextFile(outputPath)
  }
}