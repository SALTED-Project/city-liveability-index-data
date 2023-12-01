#!/usr/bin/env python
# coding: utf-8

# In[2]:


import sys
sys.path.append("/home/scorpio/ngsild-pythonclient-initial_ngsild_client/src/")

import ast
import json
from ngsildclient.client import Client
from ngsildclient.entity import Entity
import csv
import time
from datetime import datetime
import pandas as pd 
import numpy as np
from geojson.geometry import Point, Polygon, MultiLineString

client_obj = Client("localhost", "9090") # connect to scorpio

def publish(entity):
    response = client_obj.temporal_create(entity)
    #print(response)
    if response['status_code']> 400:
        print("ERROR: Cannot publish data from the NGSI client.")
    return response


# In[5]:


det_val = pd.read_csv('../cgarrido/berlin_data/traffic/det_val_hr_2022_01.csv', sep=';')


# In[ ]:


det_val


# In[6]:


mq_hr = pd.read_csv('../cgarrido/berlin_data/traffic/mq_hr_2022_01.csv', sep=';')


# In[ ]:


mq_hr


# In[7]:


traffic_stations = pd.read_excel('../cgarrido/berlin_data/traffic/traffic_station_locations.xlsx')


# In[ ]:


traffic_stations


# In[ ]:


traffic_stations.columns


# In[8]:


traffic_stn_thin = traffic_stations[['MQ_KURZNAME','LÄNGE (WGS84)', 'BREITE (WGS84)']].drop_duplicates()


# In[9]:


traffic_stn_thin = traffic_stn_thin.rename(columns={"MQ_KURZNAME": "mq_name", "LÄNGE (WGS84)": "lat", "BREITE (WGS84)": "lon"})


# In[10]:


mq_hr = mq_hr.merge(traffic_stn_thin, on = ['mq_name'])


# In[11]:


mq_hr['stunde'].unique()


# In[14]:


#qualitaet 	q_kfz_mq_hr 	v_kfz_mq_hr 	q_pkw_mq_hr 	v_pkw_mq_hr 	q_lkw_mq_hr 	v_lkw_mq_hr
num_rows_batch_publish=5000
entity_id_prefix =  "urn:ngsi-ld:Traffic:Berlin"
fac = True
for mq in list(mq_hr['mq_name'].unique()):
    entity_obj = Entity('TrafficObserved', entity_id_prefix + ':' + mq) 
    counter = 0
    station_tmp_gdf = mq_hr[mq_hr['mq_name'] == mq]
    for indx, itm in station_tmp_gdf.iterrows():
        obsdat = datetime.strptime(str(str(itm.tag) + ' ' + str(itm.stunde)), '%d.%m.%Y %H')
        entity_obj.add_prop("qualitaet",   str(itm['qualitaet'])   , observedAt= obsdat)
        entity_obj.add_prop("q_kfz_mq_hr", str(itm['q_kfz_mq_hr']) , observedAt= obsdat)
        entity_obj.add_prop("v_kfz_mq_hr", str(itm['v_kfz_mq_hr']) , observedAt= obsdat)
        entity_obj.add_prop("q_pkw_mq_hr", str(itm['q_pkw_mq_hr']) , observedAt= obsdat)
        entity_obj.add_prop("v_pkw_mq_hr", str(itm['v_pkw_mq_hr']) , observedAt= obsdat)
        entity_obj.add_prop("q_lkw_mq_hr", str(itm['q_lkw_mq_hr']) , observedAt= obsdat)
        entity_obj.add_prop("v_lkw_mq_hr", str(itm['v_lkw_mq_hr']) , observedAt= obsdat)
        entity_obj.add_geoprop("location", Point((itm.lat, itm.lon)), observedAt= obsdat)
        
        if fac == True :
            print(entity_obj)
            fac = False
            
        if (counter %num_rows_batch_publish==0 or counter == len(station_tmp_gdf)-1) and  counter!=0: # publish the batch data for entity to scorpio as the rows fulfill the number of rows needed for a batch, or the dataset has come to the end
            publish(entity_obj.to_dict()) # publish the entity
            print('Published one batch')
            entity_obj = Entity('TrafficObserved', entity_id_prefix + ':' + mq)

        counter = counter + 1
print('published the data')    


# In[ ]:




