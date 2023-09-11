import numpy as np
import pandas as pd
from nptdms import TdmsFile
import os
from dateutil import parser
import time

start_time = time.time()

# to get the parent folde name dynamically and make the code portable.
dirname = os.path.dirname(__file__)


# this function takes the tdms file and converts the file to either csv or parquet.
def csv_generator(file):
    current_file = file
    # reading the current tdms file.
    tdms_file = TdmsFile.read(dirname+"/source_folder/"+current_file)
    group = tdms_file["Event Data"]
    channel = group["Channel 1"]
    df = tdms_file.as_dataframe()  # converting the tdms file to dataframe.
    # getting the time values for the data
    time1 = channel.time_track(absolute_time=True)
    '''time2 = []
    for i in time1:
        utc_time = np.datetime_as_string(i)
        utc_time = parser.parse(utc_time)
        # coverting the time to a unix timestamp (a decimal number)
        tsp = arrow.get(utc_time).format('X')
        # date = arrow.get(tsp, 'X')
        # print(date)
        # date = date.to('Asia/Calcutta')
        # print(date)
        time2.append(tsp)'''
    dict = {}
    for i in df.columns:
        dict[i] = i[15:-1]  # Giving proper header names to the data
    df.rename(columns=dict, inplace=True)
    df["UTC_Time_Stamp"] = time1
    updated_name = file[:-4]+"parquet"  # giving the files parquet extension
    if ('Partial Loss' in updated_name):
        # Remove space from the name and replace it with an underscore.
        updated_name = updated_name.replace('Partial Loss', 'Partial_loss')
    print(updated_name)
    if "Beam_kill" in updated_name:  # adding the file to appropriate folder
        df.to_parquet(dirname+'/destination_folder' +
                      '/Beam_kill/'+updated_name, index=False)
        os.rename(dirname+'/destination_folder'+'/Beam_kill/'+updated_name,
                  dirname+'/destination_folder'+'/Beam_kill/'+'completed_'+updated_name)  # adding completed in the file ensures the transformation is complete
    elif "Partial_loss" in updated_name:
        df.to_parquet(dirname+'/destination_folder'+'/Partial_loss/' +
                      updated_name, index=False)
        os.rename(dirname+'/destination_folder'+'/Partial_loss/'+updated_name,
                  dirname+'/destination_folder'+'/Partial_loss/'+'completed_'+updated_name)
    elif "RF trip" in updated_name:
        df.to_parquet(dirname+'/destination_folder'+'/Rf_trip/' +
                      updated_name, index=False)
        os.rename(dirname+'/destination_folder'+'/Rf_trip/'+updated_name,
                  dirname+'/destination_folder'+'/Rf_trip/'+'completed_'+updated_name)
    else:
        df.to_parquet(dirname+'/destination_folder'+'/Ps_trip/' +
                      updated_name, index=False)
        os.rename(dirname+'/destination_folder'+'/Ps_trip/'+updated_name,
                  dirname+'/destination_folder'+'/Ps_trip/'+'completed_'+updated_name)

    # remove the tdms file from the source after conversion.
    os.remove(dirname+"/source_folder/"+current_file)
    print(time.time()-start_time)


# this function looks for the tdms files in the source folder and lists them.
def get_files():
    list_of_files = []
    for filename in os.listdir(dirname+'/source_folder'):
        # don't require the tdms_index files as getting all the data from the tdms files.
        if '.tdms_index' not in filename:
            list_of_files.append(filename)
        else:
            os.remove(dirname+'/source_folder/'+filename)
    return list_of_files


while (True):
    # handle file copy issue (check file locking)
    list_files_currently = get_files()
    for file in list_files_currently:
        csv_generator(file)
