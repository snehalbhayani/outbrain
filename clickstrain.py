# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd
import numpy as np

DEFAULT_PATH_TO_DATA = 'outbrainclick/'
def get_store():
    return pd.HDFStore('database.db')

def close_store(store):
    store.close()
    
def put_into_HDFStore(table, chunk_size):
    store = get_store()
    try:
        store.remove(table)
    except Exception:
        pass
    for chunk in pd.read_csv(DEFAULT_PATH_TO_DATA + table+'.csv', chunksize = chunk_size):
        print(chunk['platform'].unique())
        chunk['platform'] = chunk['platform'].apply(lambda x: 7 if x == '\\N' else x)
#        chunk[chunk['platform'] == "1"] = 4        
#        chunk[chunk['platform'] == '2' ] = 5
#        chunk[chunk['platform'] == '3' ] = 6
#        chunk[chunk['platform'] == '\\N' ] = 7
        print(chunk['platform'].unique())
        store.append(table, chunk, data_columns = chunk.columns.values)
    #close the store
    close_store(store)

def get_data_by_id(table, field, value):
    store = get_store()
    return pd.read_hdf('database', table, where=field+ '='+ value)
    close_store(store)
    
