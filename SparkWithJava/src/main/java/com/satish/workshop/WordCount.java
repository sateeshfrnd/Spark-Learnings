package com.satish.workshop.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import static java.lang.System.out;

public class WordCount {

	public static void main(String[] args) {
		Logger logger = Logger.getLogger(WordCount.class);
		
		if (args.length < 2) {
			logger.error("=> wrong parameters number");
			out.println("Usage: Pass Arguments <InputFile-Path> <OutputFile-Path>");
			System.exit(1);
		}

		String inputFile = args[0];
		String outputFile = args[1];

		// Create a Java Spark Context.
		SparkConf conf = new SparkConf().setAppName("wordCount");
//		conf.setMaster("local");

		// Create SparkContext
		JavaSparkContext jsc = new JavaSparkContext(conf);

		logger.info("=> inputPath \"" + inputFile + "\"");

		// Load input data.
		JavaRDD<String> lines = jsc.textFile(inputFile);

		logger.info("=> numLines \"" + lines.count() + "\"");

		// Split up into words.
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			public Iterable<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" "));
			}
		});

		// Transform into word and count.
		JavaPairRDD<String, Integer> wordcount = words.mapToPair(new PairFunction<String, String, Integer>() {

			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}

		});

		JavaPairRDD<String, Integer> aggreatedWordcount = wordcount
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

					public Integer call(Integer x, Integer y) throws Exception {
						return (x + y);
					}
				});

		List<Tuple2<String, Integer>> result = aggreatedWordcount.collect();
		out.println("result = " + result);

		// Save the word count back out to a text file, causing evaluation.
		 aggreatedWordcount.saveAsTextFile(outputFile);

		jsc.stop();
	}
}
