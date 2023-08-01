import pymongo
from pymongo import MongoClient
# Create the client
client = MongoClient('localhost', 27017)

# Connect to our database
db = client['streamingdb']

# Fetch our series collection
series_collection = db['streamingcoll']


def insert_document(collection, data):
    """ Function to insert a document into a collection and
    return the document's id.
    """
    return collection.insert_one(data).inserted_id


new_show = {
    "name": "FRIENDS",
    "year": 1994
}
print(insert_document(series_collection, new_show))