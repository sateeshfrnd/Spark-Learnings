spark-submit --class com.satish.workshop.spark.App \
	--master local[*] \
	target/spark1.6-0.0.1-SNAPSHOT.jar \
	'SHOW DATABASES;'

