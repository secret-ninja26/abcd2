import pandas as pd
import os
import time
import pickle
import pymongo
print("started")  # indicate that the script has started



def get_event_collection_name(event_name):
    if event_name == 'Beam Kill':
        return 'beam_kill_event_collection'
    elif event_name == 'Partial Loss':
        return 'partial_loss_event_collection'
    elif event_name == 'Ps Trip':
        return 'ps_trip_event_collection'
    else:
        return 'rf_trip_event_collection'


# provides the list of cnodes to connect to.
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["fdaq"]
collection = db["spark_temp_collection"]
metric_collection=""
filter_data_collection=db["filter_data_collection"]


#collection.drop()
#filename = "18_04_2022_12_52_54"
#channel_name = "Channel_1"
def insert_average(file_name,event_name):
    event_collection=db[get_event_collection_name(event_name)]
    channel_name_list=[]
    group_dict={'_id':None}
    for i in range(1,81):
        channel_name_list.append(f"Channel_{i}")
        group_dict[f"Channel_{i}_mean"]={'$avg':f'$Channel_{i}'}
        group_dict[f"Channel_{i}_ms"] = { '$avg': { '$pow': [f'$Channel_{i}', 2] } }
        group_dict[f"Channel_{i}_std"]={'$stdDevPop':f'$Channel_{i}'}
    
    project_dict={"_id":0,"file_name":file_name}
    for i in range(1,81):
        project_dict[f"Channel_{i}_rms"]={'$sqrt':f'$Channel_{i}_ms'}
        project_dict[f"Channel_{i}_mean"]=1
        project_dict[f"Channel_{i}_std"]=1

    filter_criteria={'file_name':file_name}
    

    pipeline = [
        {'$match':filter_criteria},
        { '$group': group_dict },
        {'$project':project_dict}
    ]

    result = event_collection.aggregate(pipeline)


    for doc in result:
        return doc


    #client.close()

def max_min_val(file_name,event_name):
    event_collection=db[get_event_collection_name(event_name)]
    answer_dict={}
    for i in range(1,81):
        project_dict={"_id":0}
        group_dict={'_id':None}
        group_dict[f"Channel_{i}_max_value"] = { '$first': f'$Channel_{i}' }
        group_dict[f"Channel_{i}_max_timestamp"] = { '$first': '$timestamp_str' }
        group_dict[f"Channel_{i}_min_value"] = { '$last': f'$Channel_{i}' }
        group_dict[f"Channel_{i}_min_timestamp"] = { '$last': '$timestamp_str' }
        filter_criteria={'file_name':file_name}

        pipeline = [
            {'$match':filter_criteria},
            {'$sort':{f'Channel_{i}':-1}},
            { '$group': group_dict },
            {'$project':project_dict}
        ]

        result = event_collection.aggregate(pipeline)
        for doc in result:
            answer_dict=answer_dict | doc
    return answer_dict



#insert_average("18_04_2022_12_52_54")
#print(max_min_val("18_04_2022_12_52_54"))

while (True):
    results_cursor = collection.find({})

    for first_document in results_cursor:
        file = first_document["file_name"]
        event_name = first_document["event_name"]
        full_metrics = insert_average(file, event_name) | max_min_val(file, event_name)
    
        if event_name == 'Beam Kill':
            metric_collection = db['beam_kill_metric_collection']
        elif event_name == 'Partial Loss':
            metric_collection = db['partial_loss_metric_collection']
        elif event_name == 'Ps Trip':
            metric_collection = db['ps_trip_metric_collection']
        else:
            metric_collection = db['rf_trip_metric_collection']
    
        metric_collection.insert_one(full_metrics)
    
        data = {
        "event_name": event_name,
        "file_name": file,
        "file_time": file[6:10] + '-' + file[3:5] + '-' + file[0:2]
        }
        print(data)
        filter_data_collection.insert_one(data)
        collection.delete_one({"_id": first_document["_id"]})
        print("done")
