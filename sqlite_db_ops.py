import sqlite3
import pymongo

def check_file_update(db_name, table_name, file_name):
    conn = sqlite3.connect(f'{db_name}.db')
    cursor = conn.cursor()
    query = f"SELECT counter FROM {table_name} WHERE file_name = ?"
    cursor.execute(query, (file_name,))
    counter_val = cursor.fetchone()
    
    if counter_val:
        cnt = int(counter_val[0]) + 1
        
        if cnt == 3:
            query = f"DELETE FROM {table_name} WHERE file_name = ?"
            cursor.execute(query, (file_name,))
            conn.commit()  # Commit changes when using DELETE
            conn.close()
            return 1
        else:
            query = f"UPDATE {table_name} SET counter = counter + 1 WHERE file_name = ?"
            cursor.execute(query, (file_name,))
            conn.commit()  # Commit changes when using UPDATE
            conn.close()
            return 2
    else:
        conn.close()
        return False

def insert_file(db_name, table_name, file_name):
    conn = sqlite3.connect(f'{db_name}.db')
    cursor = conn.cursor()
    query = f'INSERT INTO {table_name} (file_name, counter) VALUES (?, ?)'
    cursor.execute(query, (file_name, 1))
    conn.commit()  # Commit changes when using INSERT
    conn.close()

def metrics_temp_table_insert(file_name,event_name):
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["fdaq"]
    collection = db["spark_temp_collection"]
    data = {
    "file_name": file_name,
    "event_name": event_name,
    }
    collection.insert_one(data)
    client.close()
