from cassandra.query import BatchStatement
from cassandra.cluster import Cluster
import pandas as pd
import os
import time
import pickle
from cassandra import ConsistencyLevel
from cassandra.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT
profile = ExecutionProfile(consistency_level=ConsistencyLevel.LOCAL_QUORUM)


print("started")  # indicate that the script has started
start_time = time.time()

# keeps track if all 3 component files have been ingested into the db (use these 4 lines for initialization)
file_tracker = {}
dbfile = open('file_tracker_bkp', 'wb')
pickle.dump(file_tracker, dbfile)
dbfile.close()


# to get the parent folde name dynamically and make the code portable.
path = os.path.dirname(__file__)


# provides the list of cnodes to connect to.
cluster = Cluster(['14.14.14.1', '14.14.14.3'],
                  port=9042, execution_profiles={EXEC_PROFILE_DEFAULT: profile})
# indicates the keyspace to connect to.
session = cluster.connect('fdaq')
session.execute("use fdaq;")


def insert_into_cassandra(keyspace_name, table_name, columns_name_list, value_list):
    session = cluster.connect(keyspace_name)
    session.execute(f"use {keyspace_name} ;")
    query = f"insert into {table_name} ( "
    for i in range(len(columns_name_list)):
        query += (columns_name_list[i]+" , ")
    query = (query[0:-2] + ") values (")
    for i in range(len(columns_name_list)):
        query += ("?"+" , ")
    query = (query[0:-2] + ");")
    print(query)
    stmt = session.prepare(query)
    qry = stmt.bind(value_list)
    session.execute(qry)

# session.execute("drop table beam_kill_event_table_1 ;")


# create a table with unix timestamp as the PK nad the channel data.
'''query = "create table beam_kill_event_table ( file_name text, Absolute_Time timestamp , micro_seconds int"

for i in range(1, 81):
    query += ',Channel_'+str(i)+' decimal'
query += ' , primary key(file_name , Absolute_Time , micro_seconds ));'

session.execute(query)'''


# prepared statement to insert data into the table.
query_1 = "insert into beam_kill_event_table (file_name , Absolute_Time , micro_seconds"
for i in range(1, 81):
    query_1 += ',Channel_'+str(i)
query_1 += ') values(? ,? ,?'
for i in range(1, 81):
    query_1 += ',?'
query_1 += ');'

stmt = session.prepare(query_1)


folder = path+"/destination_folder/Beam_kill"  # setting the destination folder


# get the list of files in the target directory whch have been comverted to parquet format completely.
def get_files():
    list_of_files = []
    for filename in os.listdir(path+'/destination_folder/Beam_kill/'):
        if 'completed' in filename:
            list_of_files.append(filename)
    return list_of_files


# read the parquet files and insert the records into cassandra one record at a time.
def beam_kill_insert(file):

    df = pd.read_parquet(folder+'/'+file)
    for i, row in df.iterrows():
        data = []
        tsp = row['UTC_Time_Stamp']
        str_tsp = str(tsp)
        micro_seconds = str_tsp[-3:]
        # print(micro_seconds)
        # date = arrow.get(tsp, 'X')
        # print(date)
        # date = date.to('Asia/Calcutta')
        # print(date)
        data.append(file[10:29])
        data.append(tsp)
        data.append(int(micro_seconds))
        # print(float(tsp))
        for j in range(1, 81):
            key = 'Channel '+str(j)
            data.append(str(row[key]))
        qry = stmt.bind(data)
        session.execute(qry)
        # print(data[0])
        # print(type(data[0]))
        # time.sleep(5)

    # delete the parquet file after data insertion to prevent a loop.
    os.remove(path+'/destination_folder/Beam_kill/'+file)
    dbfile = open('file_tracker_bkp', 'rb')
    file_tracker = pickle.load(dbfile)
    dbfile.close()
    print(file_tracker, "o-1")
    if file[10:29] not in file_tracker:
        file_tracker[file[10:29]] = 1
        print(file_tracker, "o-2")
    else:
        file_tracker[file[10:29]] += 1
        print(file_tracker, "o-3")
        if file_tracker[file[10:29]] == 3:
            print(file_tracker, "o-4")
            insert_into_cassandra("fdaq", "spark_temp_table", [
                "file_name", "event_name"], [file[10:29], "Beam Kill"])
            # put into spark db
            del file_tracker[file[10:29]]
    dbfile = open('file_tracker_bkp', 'wb')
    pickle.dump(file_tracker, dbfile)
    dbfile.close()

    # print("success")
    print(time.time()-start_time)


while (True):
    list_of_files = get_files()
    for file in list_of_files:
        beam_kill_insert(file)
        # break
