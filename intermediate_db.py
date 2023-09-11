import sqlite3

conn = sqlite3.connect('bk.db')
cursor = conn.cursor()

create_table_query = '''
CREATE TABLE IF NOT EXISTS bk_tracker_table (
    file_name TEXT,
    counter integer
);
'''

cursor.execute(create_table_query)
conn.commit()
conn.close()

print("Table created bk successfully.")

###############################################

conn = sqlite3.connect('pl.db')
cursor = conn.cursor()

create_table_query = '''
CREATE TABLE IF NOT EXISTS pl_tracker_table (
    file_name TEXT,
    counter integer
);
'''

cursor.execute(create_table_query)
conn.commit()
conn.close()

print("Table created pl successfully.")

##########################################################

conn = sqlite3.connect('rt.db')
cursor = conn.cursor()

create_table_query = '''
CREATE TABLE IF NOT EXISTS rt_tracker_table (
    file_name TEXT,
    counter integer
);
'''

cursor.execute(create_table_query)
conn.commit()
conn.close()

print("Table created rt successfully.")

##################################################################

conn = sqlite3.connect('pt.db')
cursor = conn.cursor()

create_table_query = '''
CREATE TABLE IF NOT EXISTS pt_tracker_table (
    file_name TEXT,
    counter integer
);
'''

cursor.execute(create_table_query)
conn.commit()
conn.close()

print("Table created pt successfully.")



