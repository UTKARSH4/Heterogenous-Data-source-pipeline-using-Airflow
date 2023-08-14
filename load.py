import logging.config
import os
from pyspark.sql.types import  *
logging.config.fileConfig('properties/configuration/logging.config')

loggers = logging.getLogger('load')

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


def load_files(spark, file_dir):
    try:
        loggers.warning('load_files method started...')

        file, extension = os.path.splitext(file_dir)
        if extension == '.csv':
            df = spark.read.format('csv') \
                .option("inferSchema", True) \
                .option('header', False) \
                .load(file_dir)
            df = df.withColumnsRenamed({'_c0': 'Rowid', '_c1': 'Timestamp', '_c2': 'Anonymized_Vehicle_number',
                                        '_c3': 'Vehicle_type', '_c4': 'Number_of_axles', '_c5': 'Vehicle_code'})
        elif extension == '.tsv':
            df = spark.read.format('csv') \
                .option('sep', r'\t') \
                .option('header', False) \
                .option('inferSchema', True) \
                .load(file_dir)
            df = df.withColumnsRenamed({'_c0': 'Rowid', '_c1': 'Timestamp', '_c2': 'Anonymized_Vehicle_number',
                                        '_c3': 'Vehicle_type', '_c4': 'Number_of_axles', '_c5': 'Tollplaza_id',
                                        '_c6': 'Tollplaza_code'})
        elif extension == '.txt':
            df = spark.createDataFrame(
                spark.sparkContext.textFile(file_dir).
                map(
                    lambda x: (int(x[0:6]), str(x[7:31]), int(x[32:38]), int(x[43:47]), str(x[48:57]), str(x[58:61]),
                               str(x[62:67]))), schema_txt)

    except Exception as e:
        loggers.error('An error occured at load_files method', str(e))
        raise
    else:
        loggers.warning('dataframe created successfully')

    return df


def display_df(df, df_name):
    df = df.show()

    return df


def df_count(df, df_name):
    try:
        loggers.warning('counting the records in the {}'.format(df_name))
        df_c = df.count()

    except Exception as e:
        raise
    else:
        loggers.warning('Number of records in the {} are {}'.format(df, df_c))

    return df_c
