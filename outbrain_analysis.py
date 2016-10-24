# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import sqlite3 as sl
from clickstrain import get_data_by_where_clause as gdid
from clickstrain import put_chunk_intoHDFStore as pcih
from clickstrain import prepare_table as ptable


try:
    type(events)
except NameError:
    events = gdid('events', '', iterator = True,\
                  columns = ['display_id', 'document_id', \
                  'timestamp', 'platform', 'geo_location'],
                  chunksize = 100000)

ptable('disp_cat')
doc_ad_chunk = pd.DataFrame()
dcat = gdid('documents_categories', '', iterator = False)
i=0
for event in events:    
#    for d in dcat:        
    i += 1
    pcih('disp_cat',event.merge(dcat, on='document_id'))
    print(i)
        