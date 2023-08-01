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

    schema = StructType([StructField('col1', IntegerType(), True),
                     StructField('col2', IntegerType(), True),
                     StructField('col3', IntegerType(), True)])
    df_txt = spark.createDataFrame(
      spark.sparkContext.textFile("C:/Users/utkarsh.verma/Downloads/tolldata/payment-data.txt").\
      map(lambda x: (int(x[0:6]), int(x[32:38]), int(x[43:47]))), schema)
    df_tsv = spark.read.csv("C:/Users/utkarsh.verma/Downloads/tolldata/tollplaza-data.tsv", sep=r'\t', header=True)\
        #.select('Rowid', 'Timestamp','Anonymized_Vehicle_number', 'Vehicle_type','Number_of_axles','Tollplaza_id','Tollplaza_code')
    df_tsv.show()