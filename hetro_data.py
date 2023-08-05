from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import time


if __name__ == "__main__":
    print("Welcome to DataMaking !!!")
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka and Message Format as JSON") \
        .master("local[*]") \
        .getOrCreate()

    schema_txt = StructType([StructField('Rowid', IntegerType(), True),
                             StructField('Timestamp', StringType(), True),
                             StructField('Anonymized_Vehicle_number', IntegerType(), True),
                             StructField('Tollplaza_id', IntegerType(), True),
                             StructField('Tollplaza_code', StringType(), True),
                             StructField('Type_of_Payment_code', StringType(), True),
                             StructField('Vehicle_Code', StringType(), True),
                             ])
    schema_csv = StructType([StructField('Rowid', IntegerType(), True),
                             StructField('Timestamp', TimestampType(), True),
                             StructField('Anonymized_Vehicle_number', IntegerType(), True),
                             StructField('Vehicle_type', StringType(), True),
                             StructField('Number_of_axles', IntegerType(), True),
                             StructField('Vehicle_code', StringType(), True),
                             ])

    schema_tsv = StructType([StructField('Rowid', IntegerType(), True),
                             StructField('Timestamp', TimestampType(), True),
                             StructField('Anonymized_Vehicle_number', IntegerType(), True),
                             StructField('Vehicle_type', StringType(), True),
                             StructField('Number_of_axles', IntegerType(), True),
                             StructField('Tollplaza_id', IntegerType(), True),
                             StructField('Tollplaza_code', StringType(), True),
                             ])

    df_txt = spark.createDataFrame(
      spark.sparkContext.textFile("C:/Users/utkarsh.verma/Downloads/tolldata/payment-data.txt").\
      map(lambda x: (int(x[0:6]), str(x[7:31]), int(x[32:38]), int(x[43:47]), str(x[48:57]), str(x[58:61]),
                     str(x[62:67]))), schema_txt)
    df_tsv = spark.read.format('csv')\
                       .option('sep', r'\t')\
                       .option('header', False)\
                       .option('inferSchema', True)\
                       .load("C:/Users/utkarsh.verma/Downloads/tolldata/tollplaza-data.tsv")
    df_tsv = df_tsv.withColumnsRenamed({'_c0': 'Rowid', '_c1': 'Timestamp', '_c2': 'Anonymized_Vehicle_number',
                                        '_c3': 'Vehicle_type', '_c4': 'Number_of_axles', '_c5': 'Tollplaza_id',
                                        '_c6': 'Tollplaza_code'})
    df_tsv.show()
    df_csv = spark.read.format('csv') \
                       .option("inferSchema", True
                               ) \
                       .option('header', False) \
                       .load("C:/Users/utkarsh.verma/Downloads/tolldata/vehicle-data.csv")
    df_csv = df_csv.withColumnsRenamed({'_c0': 'Rowid', '_c1': 'Timestamp', '_c2':'Anonymized_Vehicle_number',
                                        '_c3': 'Vehicle_type', '_c4': 'Number_of_axles', '_c5': 'Vehicle_code'})
    df_csv.show()
    df_txt.show()
