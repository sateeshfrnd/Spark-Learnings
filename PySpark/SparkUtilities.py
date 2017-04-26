'''
Created on Apr 25, 2017

@author: Satish Kumar
'''
from pyspark.sql.context import HiveContext
from pyspark.sql.functions import col

def getHiveTableTargetLocation(spark,hiveTableWithSchema):
    '''
    Function returns the Hive target Location of the Hive table
    spark: hiveContext or SparkSession (From Spark 2.0)
    hiveTableWithSchema: <DB>.<TABLE>
    '''
    df = spark.sql("describe formatted {schema_table}".format(schema_table=hiveTableWithSchema))
    if isinstance(spark,HiveContext):
        loc_df = df.filter(col('result').like("%Location%")).head()
        return loc_df.result.split("\t")[1]
    else:
        loc = df.filter(df.col_name == 'Location:').select(df.data_type.alias('targetLoc')).head()
        return str(loc.targetLoc)
    
