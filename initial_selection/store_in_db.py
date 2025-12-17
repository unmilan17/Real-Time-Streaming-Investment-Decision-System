from pymongo import MongoClient
import pathway as pw
from news_consumer_src.news_consumer import *

import warnings
warnings.filterwarnings("ignore")


url = "mongodb://mongo:27017"

client = MongoClient(url)

db = client["news_database"]
collection = db["news_collection"]

data = news_table

collection.insert_one({"init": True})
collection.delete_one({"init": True})

pw.io.mongodb.write(
    data,
    connection_string=url,
    database=db,
    collection=collection,
)






