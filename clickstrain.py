# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd
import numpy as np


def get_store():
    return pd.HDFStore('database')

def close_store(store):
    store.close()
    
def put_into_HDFStore(table, chunk_size):
    store = get_store()
    try:
        store.remove(table)
    except Exception:
        pass
    for chunk in pd.read_csv(table+'.csv', chunksize = chunk_size):
        print(chunk.shape[0])
        store.append(table, chunk, data_columns = chunk.columns.values)
    #close the store
    close_store(store)

def get_data_by_id(table, field, value):
    store = get_store()
    return pd.read_hdf('database', table, where=field+ '='+ value)
    close_store(store)