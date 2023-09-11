import pandas as pd
import os
import time
import pickle
import pymongo
from sqlite_db_ops import *

db_name='rt'
table_name='rt_tracker_table'



print("started")  # indicate that the script has started
start_time = time.time()

# keeps track if all 3 component files have been ingested into the db (use these 4 lines for initialization)




# to get the parent folde name dynamically and make the code portable.
path = os.path.dirname(__file__)


# provides the list of cnodes to connect to.
client = pymongo.MongoClient("mongodb://localhost:27017/")

db = client["fdaq"]

# Create or use a collection
collection = db["rf_trip_event_collection"]

collection.create_index([("file_name", pymongo.ASCENDING), ("timestamp_str", pymongo.ASCENDING)], unique=True)




folder = path+"/destination_folder/Rf_trip"  # setting the destination folder


# get the list of files in the target directory whch have been comverted to parquet format completely.
def get_files():
    list_of_files = []
    for filename in os.listdir(path+'/destination_folder/Rf_trip/'):
        if 'completed' in filename:
            list_of_files.append(filename)
    return list_of_files


# read the parquet files and insert the records into cassandra one record at a time.
def rf_trip_insert(file):
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["fdaq"]
    collection = db["rf_trip_event_collection"]
    data = []
    df = pd.read_parquet(folder+'/'+file)
    for i, row in df.iterrows():
        dict={}
        dict["timestamp"] = row['UTC_Time_Stamp']
        dict["timestamp_str"]=str(row['UTC_Time_Stamp'])
        dict["file_name"]=file[10:29]
        for j in range(1, 81):
            key = 'Channel_'+str(j)
            key_og='Channel '+str(j)
            dict[key]=row[key_og]
        data.append(dict)
        if len(data)>10000:
            #print(data)
            collection.insert_many(data)
            data=[]
    if len(data)!=0:
        collection.insert_many(data)
        data=[]
    client.close()
    # delete the parquet file after data insertion to prevent a loop.

    #os.remove(path+'/destination_folder/Beam_kill/'+file)
    print("o-1")
    result=check_file_update(db_name,table_name,file[10:29])
    if not result:
        insert_file(db_name,table_name,file[10:29])
        os.remove(path+'/destination_folder/Rf_trip/'+file)
        print("o-2")
    elif result==1:
        print("deleted")
        metrics_temp_table_insert(file[10:29],"Rf Trip")
        os.remove(path+'/destination_folder/Rf_trip/'+file)
        
        # put into spark db
    else:
        print("updated")
        os.remove(path+'/destination_folder/Rf_trip/'+file)

    # print("success")
    print(time.time()-start_time)


while (True):
    list_of_files = get_files()
    for file in list_of_files:
        rf_trip_insert(file)
        # break
