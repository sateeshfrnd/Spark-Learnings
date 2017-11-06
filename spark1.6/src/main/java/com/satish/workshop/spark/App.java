package com.satish.workshop.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {
		
		String query = args[0];
		
		// Create a Java Spark Context.
		SparkConf conf = new SparkConf().setAppName("SparkTest");

		// Create SparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Create SQLContext
		SQLContext sqlContext = new SQLContext(sc);
		
		// Create HiveContext
		HiveContext hiveContext = new HiveContext(sc);

		String sparkVersion = sc.version();
		System.out.println("Spark Version = " + sparkVersion);
		
		DataFrame df =  hiveContext.sql(query);
		df.printSchema();
		df.show(false);
	}
}
