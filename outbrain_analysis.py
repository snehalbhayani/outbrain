# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import sqlite3 as sl
from clickstrain import get_data_by_where_clause as gdid

document_id = 1711431
#==============================================================================
events= gdid('events', 'document_id='+str(document_id))
#page_views = gdid('page_views_sample', 'document_id='+str(document_id))
uuids = list(events['uuid'].get_values())
#==============================================================================
displays = list(events['display_id'].get_values())
train = gdid('clicks_train', 'display_id='+str(displays))
clicked = train[train['clicked'] == 1]
notclicked = train[train['clicked'] == 0]
#notclicked = gdid('clicks_train', 'display_id='+str(displays)+' & clicked = 0')



#================================REPORT========================================
# document_id -> events for the document -> users who visited that document -- defined by display_ids
# display_ids -> ads which were clicked -> defined by clicked
# display_ids -> ads which were not clicked -> defined by notclicked
# users -> all the events that those users did
#==============================================================================
