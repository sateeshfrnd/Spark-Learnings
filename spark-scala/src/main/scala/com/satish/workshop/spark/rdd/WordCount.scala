package com.satish.workshop.spark.rdd

import com.satish.workshop.spark.common.SparkApp
import org.apache.log4j.Logger


object WordCount extends SparkApp {
  def main(arg: Array[String]): Unit = {
    var logger = Logger.getLogger(this.getClass())

    var inputPath : String = ""
    var outputPath : String = ""

    if(!runningOnCluster) {
      inputPath = "src/main/resources/wordcount-Input.txt"
      outputPath = "src/main/resources/wc-output"
    } else {
      if (arg.length < 2) {
        logger.error("=> wrong parameters number")
        println("Usage: Pass Arguments <InputFile-Path> <OutputFile-Path>")
        System.exit(1)
      }
    }

    val lines = sparkContext.textFile(inputPath)
    val numLines = lines.map(x => x.size)
    println("Number of Lines = "+numLines)

    val wc = lines.flatMap { l => l.split(" ") }
      .map { word => (word, 1) }
      .reduceByKey(_ + _)

    wc.saveAsTextFile(outputPath)
  }
}