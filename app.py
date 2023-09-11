from flask import Flask, render_template, jsonify, request
import datetime
import pandas as pd
import pymongo



app = Flask(__name__)




@app.get('/')
def home():
    return render_template('index.html')


@app.post('/get_plot')
def plot_chart():
    event = request.get_json()
    #print(event)
    event_type = event['Event_Type']
    event_date = event['Event_Date']
    event_file = event['Event_File']
    channel_list = event['channel_list']

    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["fdaq"]

    table_name = ''
    if event_type == 'Beam Kill':
        table_name = 'beam_kill_event_collection'
    elif event_type == 'Ps Trip':
        table_name='ps_trip_event_collection'
    elif event_type == 'Rf Trip':
        table_name='rf_trip_event_collection'
    else:
        table_name = 'partial_loss_event_collection'

    event_collection = db[table_name]
    #print(table_name)
    channels_in_list_form = []
    for channel in channel_list:
        channels_in_list_form.append(f'Channel_{channel}')
    # print(channels_in_list_form)
    query = {'file_name':event_file}
    #print(query)
    projection={'_id':0,'timestamp_str':1}
    for i in channels_in_list_form:
        projection[i]=1
    
    cursor = event_collection.find(query,projection)
    arr_x = []
    channel_list_dict = {}
    for i in channels_in_list_form:
        channel_list_dict[i] = []
    for doc in cursor:
        #print(doc)
        arr_x.append(doc['timestamp_str']+'&nbsp;')
        for i in range(len(channels_in_list_form)):
            channel_list_dict[channels_in_list_form[i]].append(doc[channels_in_list_form[i]])
    client.close()
    result_dict = {}
    result_dict['arr_x'] = arr_x
    for i in channels_in_list_form:
        result_dict[i] = channel_list_dict[i]
    #print(result_dict)
    return result_dict


@app.post('/event_file')
def get_event_file_filter():
    event = request.get_json()
    #print(event)
    event_type = event['Event_Type']
    event_date = event['Event_Date']
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["fdaq"]
    filter_collection = db["filter_data_collection"]
    query = {'event_name': event_type,'file_time':event_date}
    projection={'_id':0,'file_name':1}
    cursor = filter_collection.find(query,projection)
    dict = {"file_names": []}
    for doc in cursor:
        dict['file_names'].append(doc['file_name'])
    client.close()
    return jsonify(dict)


@app.post('/on_load')
def plot_chart_on_load():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["fdaq"]
    filename = None
    filter_collection = db["filter_data_collection"]
    query = {'event_name': 'Beam Kill'}
    projection={'_id':0,'file_name':1}
    cursor = filter_collection.find(query,projection)
    for doc in cursor:
        filename = doc["file_name"]
        break
        # break (needed to get latest filename)
    arr_x = []
    arr_y = []
    arr_dcct = []
    if filename:
        event_collection = db["beam_kill_event_collection"]
        query = {'file_name': filename}
        projection={'_id':0,'Channel_1':1,'Channel_77':1,'timestamp_str':1}
        cursor = event_collection.find(query,projection)
        for doc in cursor:
            arr_x.append(doc['timestamp_str']+'&nbsp;')
            arr_y.append(doc['Channel_1'])
            arr_dcct.append(doc['Channel_77'])
    client.close()
    dict = {}
    dict['arr_x'] = arr_x
    dict['arr_y'] = arr_y
    dict['arr_dcct'] = arr_dcct
    dict['file_name'] = filename
    dict['file_date'] = filename[6:10]+'-'+filename[3:5]+'-'+filename[0:2]

    return dict


@app.post('/get_metrics_on_load')
def metrics_on_load():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["fdaq"]
    filename = None
    filter_collection = db["filter_data_collection"]
    query = {'event_name': 'Beam Kill'}
    projection={'_id':0,'file_name':1}
    cursor = filter_collection.find(query,projection)
    for doc in cursor:
        filename = doc["file_name"]
        break
    if filename:
        event_collection = db["beam_kill_metric_collection"]
        query = {'file_name': filename}
        #query = f"select channel_1,channel_77 from partial_loss_metric_table where file_name='{filename}';"
        cursor = event_collection.find(query)
        for doc in cursor:
            #channel_1_data = row.channel_1
            #channel_77_data = row.channel_77
            result_dict = {'channel_1': {}, 'channel_77': {}}
            result_dict['channel_1']['max_val'] = round(float(doc['Channel_1_max_value']), 4)
            result_dict['channel_1']['max_val_time'] = doc['Channel_1_max_timestamp']
            result_dict['channel_1']['min_val'] = round(float(doc['Channel_1_min_value']), 4)
            result_dict['channel_1']['min_val_time'] = doc['Channel_1_min_timestamp']
            result_dict['channel_1']['mean_squared_value'] = round(float(doc['Channel_1_rms']), 4)
            result_dict['channel_1']['mean_value'] = round(float(doc['Channel_1_mean']), 4)
            result_dict['channel_1']['std_value'] = round(float(doc['Channel_1_std']), 7)

            result_dict['channel_77']['max_val'] = round(float(doc['Channel_77_max_value']), 4)
            result_dict['channel_77']['max_val_time'] = doc['Channel_77_max_timestamp']
            result_dict['channel_77']['min_val'] = round(float(doc['Channel_77_min_value']), 4)
            result_dict['channel_77']['min_val_time'] = doc['Channel_77_min_timestamp']
            result_dict['channel_77']['mean_squared_value'] = round(float(doc['Channel_77_rms']), 4)
            result_dict['channel_77']['mean_value'] = round(float(doc['Channel_77_mean']), 4)
            result_dict['channel_77']['std_value'] = round(float(doc['Channel_77_std']), 7)
            client.close()
            return result_dict


@app.post('/get_metrics')
def get_metrics():
    event = request.get_json()
    #print(event)
    event_type = event['Event_Type']
    event_file = event['Event_File']
    channel_list = event['channel_list']
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["fdaq"]
    table_name = ''
    if event_type == 'Beam Kill':
        table_name = 'beam_kill_metric_collection'
    elif event_type=='Rf Trip':
        table_name='rf_trip_metric_collection'
    elif event_type=='Ps Trip':
        table_name='ps_trip_metric_collection'
    else:
        table_name = 'partial_loss_metric_collection'
    channels_in_list_form = []
    for channel in channel_list:
        channels_in_list_form.append(f'Channel_{channel}')
    
    metric_collection = db[table_name]
    query = {'file_name':event_file}
    #projection={'_id':0,'timestamp_str':1}
    #for i in channels_in_list_form:
    #    projection[i]=1
    
    cursor = metric_collection.find(query)

    list_metric_cw = []
    for i in range(0, len(channels_in_list_form)):
        list_metric_cw.append({'channel_no': channels_in_list_form[i]})

    for doc in cursor:
        for i in range(0, len(channels_in_list_form)):
            list_metric_cw[i]['max_val'] = round(float(doc[channels_in_list_form[i]+'_max_value']), 4)
            list_metric_cw[i]['max_val_time'] = doc[channels_in_list_form[i]+'_max_timestamp']
            list_metric_cw[i]['min_val'] = round(float(doc[channels_in_list_form[i]+'_min_value']), 4)
            list_metric_cw[i]['min_val_time'] = doc[channels_in_list_form[i]+'_min_timestamp']
            list_metric_cw[i]['mean_squared_value'] = round(float(doc[channels_in_list_form[i]+'_rms']), 4)
            list_metric_cw[i]['mean_value'] = round(float(doc[channels_in_list_form[i]+'_mean']), 4)
            list_metric_cw[i]['std_value'] = round(float(doc[channels_in_list_form[i]+'_std']), 7)
    client.close()
    return {'metrics': list_metric_cw}


if __name__ == '__main__':
    app.run()
