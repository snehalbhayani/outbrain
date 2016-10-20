# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd
import numpy as np
import sqlite3 as sl



DEFAULT_PATH_TO_DATA = 'outbrainclick/'
DATABASE_NAME = 'database.db'
def get_store():
    return pd.HDFStore(DATABASE_NAME)

def close_store(store):
    store.close()
    
def put_into_HDFStore(table, chunk_size):
    store = get_store()
    try:
        store.remove(table)
    except Exception:
        pass
    i = 0
    for chunk in pd.read_csv(DEFAULT_PATH_TO_DATA + table+'.csv', chunksize = chunk_size):
        print(i)
        i = i + 1
#        sanitize_data(chunk, table, 'platform')        
        store.append(table, chunk, data_columns = chunk.columns.values)
    #close the store
    close_store(store)
    
def put_into_sqlite(table, chunk_size):
    conn = sl.connect('sqldatabase.db')    
    i = 0
    for chunk in pd.read_csv(DEFAULT_PATH_TO_DATA + table+'.csv', chunksize = chunk_size):
        print(i)
        i = i + 1
#        sanitize_data(chunk, table, 'platform')        
        chunk.to_sql(table, conn, if_exists = 'append')
    conn.close()
    
# Load the table    
#put_into_sqlite('clicks_test', 10000000)

def get_data_by_id(table, field, value):
    return pd.get_data_by_where_clause(table, field+ '='+ value)
    
def get_data_by_where_clause(table, where_clause):
    store = get_store()
    if where_clause:
        result = pd.read_hdf(DATABASE_NAME, table, where=where_clause)
    else:
        result = pd.read_hdf(DATABASE_NAME, table)
    close_store(store)
    return result
    
                                                                                                                                                                                    
def sanitize_data(chunk, table, field):
    if table == 'events':
        chunk[field] = chunk[field].apply(lambda x: 7 if x == '\\N' else x)
        chunk[field] = chunk[field].apply(lambda x: int(x))
        

def inner_join_tables(table1_name,table2_name):
    table1 = pd.read_hdf(DATABASE_NAME , table1_name, iterator=True)
    table2 = pd.read_hdf(DATABASE_NAME , table2_name, iterator=True)
    return table1
    
    

    
    
    
    
    
    
    
    
    
    
    
    
