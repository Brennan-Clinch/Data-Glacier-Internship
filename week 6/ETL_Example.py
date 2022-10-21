#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import psycopg2

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType, TimestampType, ShortType, DateType
from pyspark.sql.functions import col

def main():

    # establish a connection to the db
    conn = psycopg2.connect(
        host = "localhost",
        database = "usedcars",
        user = "postgres",
        password = "postgres")

    print("Connection to PostgreSQL created", "\n")

    # create a cursor out of a connection; a cursor allows you to communicate with Postgres and execute commands
    cur = conn.cursor()

    spark = initialize_Spark()

    df = loadDFWithSchema(spark)

    create_table(cur)

    insert_query, cars_seq = write_postgresql(df)

    cur.execute(insert_query, cars_seq)

    print("Data inserted into PostgreSQL", "\n")

    get_insterted_data(cur)

    cur.close()


    print("Commiting changes to database", "\n")
    # make sure that your changes are shown in the db
    conn.commit()

    print("Closing connection", "\n")

    # close the connection
    conn.close()

    print("Done!", "\n")


def initialize_Spark():

    spark = SparkSession.builder         .master("local[*]")         .appName("Simple etl job")         .getOrCreate()

    print("Spark Initialized", "\n")

    return spark

def loadDFWithoutSchema(spark):

    df = spark.read.format("csv").option("header", "true").load('used_carsnew.csv')

    return df

def loadDFWithSchema(spark):

    schema = StructType([
        StructField("vin", StringType(), True),
        StructField("bodytype", StringType(), True),
        StructField("daysonmarket", LongType(), False),
        StructField("enginedisplacement", LongType(), True),
        StructField("enginetype", StringType(), True),
        StructField("exteriorcolor", StringType(), True),
    ])

    df = spark         .read         .format("csv")         .schema(schema)                 .option("header", "true")         .load("used_carsnew.csv")

    print("Data loaded into PySpark", "\n")

    return df

def create_table(cursor):

    try:
        cursor.execute("CREATE TABLE IF NOT EXISTS cars_table     (   vin VARCHAR(30) NULL,         bodytype VARCHAR(30) NULL,         daysonmarket int NULL,         enginedisplacement DECIMAL(10,2) NULL,         enginetype VARCHAR(5) NULL,         exteriorcolor VARCHAR(40) NULL);")

        print("Created table in PostgreSQL", "\n")
    except:
        print("Something went wrong when creating the table", "\n")


def write_postgresql(df):

    cars_seq = [tuple(x) for x in df.collect()]

    records_list_template = ','.join(['%s'] * len(cars_seq))

    insert_query = "INSERT INTO cars_table (vin, bodytype, daysonmarket, enginedisplacement, enginetype, exteriorcolor                            ) VALUES {}".format(records_list_template)

    print("Inserting data into PostgreSQL...", "\n")

    return insert_query, cars_seq

def get_insterted_data(cursor):

    postgreSQL_select_Query = "select bodytype, daysonmarket, enginedisplacement, enginetype from cars_table"

    cursor.execute(postgreSQL_select_Query)

    cars_records = cursor.fetchmany(2)

    print("Printing 2 rows")
    for row in cars_records:
        print("bodytype = ", row[0], )
        print("daysonmarket = ", row[1])
        print("enginedisplacement  = ", row[2])
        print("enginetype  = ", row[3], "\n")


if __name__ == '__main__':
    main()


# In[ ]:




