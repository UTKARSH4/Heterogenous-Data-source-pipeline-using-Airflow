from pymongo import MongoClient
import logging.config

logging.config.fileConfig('properties/configuration/logging.config')

loggers = logging.getLogger('mongo_connect')

# Create the client
client = MongoClient('localhost', 27017)

# Connect to our database
db = client['streamingdb']

# Fetch our series collection
series_collection = db['streamingcoll']


def insert_document(df, df_name):

    try:
        loggers.warning('insert_document method started...')
        df.write.format("mongodb") \
            .option("spark.mongodb.connection.uri", "mongodb://localhost:27017/")\
            .option("spark.mongodb.database", "streamingdb")\
            .option("spark.mongodb.collection", "streamingcoll")\
            .mode("append").save()

    except Exception as e:
        loggers.error('insert_document method failed ...', str(e))
        raise
    else:
        loggers.warning('inserted {}..'.format(df_name))
