#===================================================================================================================================================
'''

@author: Satish Kumar

JDBCLoader fetch the data from the source using ConnectionURL and JDBC Driver and save to Hive table or specified location.
Parameters :
CON_URL     : Connection URL
DRIVER      : JDBC Driver class
TABLENAME   : Name of the table.
USERNAME    : Username to connect to source
PASSWORD    : Password to connect to source
HIVE_DB     : Name of the database
HIVe_TBL    : Name of the table.

Referance   : https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html

Usage:
spark-submit --name LoadData \
	 --master yarn \
     --deploy-mode cluster  \
     --keytab <KERB_KEYTAB_FILE>.keytab \
     --principle <KERB_PRINCIPLE> \
     JDBCLoader.py <CON_URL> <DRIVER> <TABLENAME> <USERNAME> <PASSWORD> <HIVE_DB> <HIVe_TBL> <MODE>
'''
#===================================================================================================================================================
import sys
from sys import argv
from pyspark.sql import SparkSession

'''
Fetch the data from source to spark dataframe
spark       : Spark Session 
connString  : Connection URL
driver      : JDBC Driver class
tableName   : Name of the table.
username    : Username to connect to source
password    : Password to connect to source 
'''
def fetchDataFromSource(spark, connString, driver, tableName, username, password) :
    query = "Select * from " + tableName
    df = spark.read.format("jdbc").option("url", connString).option("dbtable", f"({query})").option("user", username).option(
        "password", password).option("driver", driver).load()
    return df

'''
Checks is table exists in the database
spark       : Spark Session 
dbName      : Name of the database
tableName   : Name of the table.
'''
def isTableExists(spark, dbName, tableName):
    checkQuery = f"show tables in {dbName} like '{tableName}'"
    if(spark.sql(checkQuery).count() > 0):
        return True
    return False

'''
Save dataframe to a table or location
spark       : Spark Session 
df          : Dataframe to save
dbName      : Name of the database
tableName   : Name of the table.
overwite    : Mode - ovrwrite/append
'''
def saveDataFrame(spark, df, targetLoc, dbName=None, tableName=None, overwrite=True):
    if(overwrite == True):
        mode = "overwrite"
    else:
        mode = "append"

    # https://spark.apache.org/docs/latest/sql-data-sources-parquet.html
    spark.conf.set("spark.sql.parquet.writeLegacyFormat","true")

    if(dbName != None and tableName != None):
        table = f"{dbName}.{tableName}"
        if(isTableExists(dbName,tableName)) :
            print(f"Table '{table}' exists")
            df.write.format("parquet").mode(mode).option("path", targetLoc).insertInto(table)
        else:
            df.write.format("parquet").mode(mode).option("path", targetLoc).saveAsTable(table)
        print(f"Saved Dataframe to {table} pointing to {targetLoc} with mode {mode}")
    else:
        df.write.mode(mode).parquet(targetLoc)
        print(f"Saved Dataframe to the location {targetLoc} with mode {mode}")


def main(argv):
    print(argv)
    connString = argv[0]
    driver = argv[1]
    tableName = argv[2]
    username = argv[3]
    password = argv[4]
    targetLoc = argv[5]

    print(f"connString = {connString}, driver = f{driver}, tableName = {tableName}, username = {username}, password = {password}, targetLoc= {targetLoc}")
    hiveDB = None
    hiveTbl=None
    mode=None

    if (len(argv) == 8):
        hiveDB = argv[6]
        hiveTbl = argv[7]
        print(f"hiveDB = {hiveDB}, hiveTbl = f{hiveTbl}")

    if (len(argv) ==9):
        mode = argv[8]
        print(f"mode = {mode}")

    print("Initialising Spark Session")
    spark = SparkSession \
        .builder \
        .appName("JDBC Loader") \
        .enableHiveSupport() \
        .getOrCreate()

    df = fetchDataFromSource(spark, connString, driver, tableName, username, password)
    saveDataFrame(spark, df, targetLoc, hiveDB, hiveTbl, True)
    # saveDataFrame(spark,df, targetLoc, dbName, tableName, True) - Save df to the location and load to Hive table and overwrite if data exists
    # saveDataFrame(spark,df, targetLoc, dbName, tableName) - Save df to the location and load to Hive table and overwrite if data exists
    # saveDataFrame(spark,df, targetLoc, dbName, tableName, False) - Save df to the location and load to Hive table and append data if exists
    # saveDataFrame(spark,df, targetLoc) - Save df to the location and overwrite if data exists
    # saveDataFrame(spark,df, targetLoc,False) - Save df to the location and append data if exists


if __name__ == "__main__":
    if len(sys.argv) < 6:
        raise Exception('Incorrect number of arguments passed')
    print('Number of arguments:', len(sys.argv), 'arguments.')
    print('Argument List:', str(sys.argv))
    main(sys.argv[1:])
