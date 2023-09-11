import pandas as pd
import os
import time
import pickle
import pymongo



print("started")  # indicate that the script has started




# provides the list of cnodes to connect to.
client = pymongo.MongoClient("mongodb://localhost:27017/")

db = client["fdaq"]

# Create or use a collection
collection = db["rf_trip_event_collection"]


#collection.drop()
'''filename = "18_04_2022_12_52_54"
channel_name = "Channel 1"'''

# Query data for the given filename
#query = {"file_name":"14_07_2023_06_42_26"}
query={'file_name': '14_07_2023_06_42_26'}
projection = {"_id": 0}

cursor = collection.find(query,projection)

#Extract and print channel values
for i in cursor:
    print(i)


client.close()


