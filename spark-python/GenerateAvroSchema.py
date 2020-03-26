'''
Created on March 26, 2020

@author: Satish Kumar

Description : Generate AVRO Schema from the Parquet File.

Usage:
spark-submit --name GenerateAvroSchema \
	 --master yarn \
     --deploy-mode cluster  \
     --keytab <KERB_KEYTAB_FILE>.keytab \
     --principle <KERB_PRINCIPLE> \
     GenerateAvroSchema.py <NAME> <NAMESPACE> <DATA_LOCATION_PATH>
'''

import sys
from sys import argv
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, TimestampType, BooleanType, DateType, DoubleType, FloatType, ByteType, IntegerType, LongType, ShortType, ArrayType, MapType, DecimalType
import datetime

def convertColType(ctype):
	if (isinstance(ctype, StringType)):
		resultType = "string"
	elif (isinstance(ctype, TimestampType)):
		resultType = "timestamp"
	elif (isinstance(ctype, BooleanType)):
		resultType = "boolean"
	elif (isinstance(ctype, DateType)):
		resultType = "date"
	elif (isinstance(ctype, DoubleType)):
		resultType = "double"
	elif (isinstance(ctype, FloatType)):
		resultType = "float"
	elif (isinstance(ctype, ByteType)):
		resultType = "byte"
	elif (isinstance(ctype, IntegerType)):
		resultType = "integer"
	elif (isinstance(ctype, LongType)):
		resultType = "long"
	elif (isinstance(ctype, ShortType)):
		resultType = "short"
	elif (isinstance(ctype, ArrayType)):
		resultType = "array"
	elif (isinstance(ctype, MapType)):
		resultType = "map"
	elif (isinstance(ctype, DecimalType)):
		resultType = "decimal("+str(ctype.precision)+","+str(ctype.scale)+")"
	else:
		resultType = ctype
	return resultType

def generateAvroSchema(spark, argv):
	name = argv[0]
	namespace = argv[1]
	dataLocation = argv[2]

	print("Input Parameters : ")
	print("name          = ", name)
	print("namespace          = ", namespace)
	print("dataLocation        = ", dataLocation)

	df = spark.read.parquet(dataLocation)

	print("Generating Avro Schema File...")
	data_schema = [(f.name, f.tdataType) for f in df.schema.fields]
	print(data_schema)

	avro_schema = dict()
	avro_schema["type"] = "record"
	avro_schema["name"] = name
	avro_schema["namespace"] = namespace
	fields = []
	for kv in data_schema:
		col_details = dict()
		col_details["name"] = kv[0]
		col_details["type"] = convertColType(kv[1])
		fields.append(col_details)
	avro_schema["fields"] = fields
	print(avro_schema)
	final_schema = json.dumps(avro_schema, indent=4, skipkeys=True)
	print(type(final_schema))
	print(final_schema)


def main(argv):

	print("Initialising Spark Session")
	spark = SparkSession \
		.builder \
		.appName("Generating Control File") \
		.enableHiveSupport() \
		.getOrCreate()

	sc = spark.sparkContext
	generateAvroSchema(spark, argv)

if __name__ == "__main__":
	if len(sys.argv) != 5:
		raise Exception('Incorrect number of arguments passed')
	print('Number of arguments:', len(sys.argv), 'arguments.')
	print('Argument List:', str(sys.argv))
	main(sys.argv[1:])


