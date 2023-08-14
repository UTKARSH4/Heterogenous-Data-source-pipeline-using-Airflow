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


def insert_document(collection, data):
        try:
            loggers.warning('insert_document method started...')
            id=  collection.insert_one(data).inserted_id

        except Exception as e:
            loggers.error('insert_document method failed ...', str(e))
            raise
        else:
            loggers.warning('insertion done..')
