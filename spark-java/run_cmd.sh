spark-submit --class com.satish.learnings.spark_java.spark.WordCount \
	--master local[*] \
	target/spark-java-maven-0.0.1-SNAPSHOT.jar \
	'/home/satish/MyWorkshop/Sample-Inputs/wordcount-Input.txt' \
	'/home/satish/MyWorkshop/Sample-Inputs/wccount'

