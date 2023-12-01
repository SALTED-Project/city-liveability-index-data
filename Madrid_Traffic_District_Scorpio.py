#!/usr/bin/env python
# coding: utf-8

# In[21]:


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



# In[22]:


num_rows_batch_publish=5000 # number of objects to be batch published to Scorpio


# # Notes:
# Consider every hexagon as an entity, and sensor data as properties

# In[23]:


import pandas as pd
import geopandas as gpd

from geojson.geometry import Point, Polygon, MultiLineString


# In[24]:


#gdf_NO2 = pd.read_pickle('./../cgarrido/madrid_data/air_pollution/gdf_aggr_NO2.pkl')
#gdf_O3 = pd.read_pickle('./../cgarrido/madrid_data/air_pollution/gdf_aggr_O3.pkl')
#gdf_PM10 = pd.read_pickle('./../cgarrido/madrid_data/air_pollution/gdf_aggr_PM10.pkl')
#gdf_PM2_5 = pd.read_pickle('./../cgarrido/madrid_data/air_pollution/gdf_aggr_PM2.5.pkl')
#gdf_SO2 = pd.read_pickle('./../cgarrido/madrid_data/air_pollution/gdf_aggr_SO2.pkl')
#gdf_CO = pd.read_pickle('./../cgarrido/madrid_data/air_pollution/gdf_aggr_CO.pkl')



# In[ ]:





# In[25]:


gdf_trr = pd.read_pickle('./../cgarrido/madrid_data/traffic/gdf_aggr_traffic_urb.pkl')


# In[26]:


gdf_trr_2 = pd.read_pickle('./../cgarrido/madrid_data/traffic/gdf_aggr_traffic_m30_month_1.pkl')


# In[27]:


gdf_trr.columns


# In[28]:


#gdf_list= [gdf_PM2_5,  gdf_NO2, gdf_O3, gdf_PM10, gdf_SO2, gdf_CO]
#gdf_property_list=["PM2_5", "NO2", "O3", "PM10",  "SO2", "CO"]
#entity_id_prefix =  "urn:ngsi-ld:AirQualityObserved:Madrid"
#entity_type = 'AirQualityObserved'

gdf_property_list=['traffic_intensity_urb','traffic_occupation_urb', 'traffic_load_urb']
entity_id_prefix =  "urn:ngsi-ld:TrafficObservedDistrictLevel:Madrid:"
entity_type = 'TrafficObservedDistrictLevel'


# 1- Check context and add context (smart data model link) to the @context tag
# 
# 2- Geometry: Change geometry to location. Location is a geoproperty. Give the point values as "coordinates". Keep the observedat on every property.
# 
# 3- Convert coordinates to lon/lat
# 
# 4- Add no2, so2 as property (not as a separate entity). One entity per hexagon
# . 
# 5- Add district instead of hexagons
# 

# In[29]:


example_flag=False
ingestion_starttime= time.perf_counter() 
example_flag=True
#prop =gdf_property_list[property_index]

hexagon_id_list = gdf_trr['hexagon_id'].unique()
print("Info: Unique hexagons for the dataset : "+ str(hexagon_id_list))
feature_list= gdf_trr.columns.tolist()
dataset_length = len(gdf_trr)
for hexagon_id in hexagon_id_list:
    hexagon_gdf = gdf_trr[gdf_trr['hexagon_id']== hexagon_id]

    entity_obj = Entity(entity_type, entity_id_prefix + ':Hexagon:' + str(hexagon_id)) # (re-)initialize the entity

    counter =0
    for index, row in hexagon_gdf.iterrows():
        lat=0
        lon=0
        if counter %num_rows_batch_publish==0: # create a new clean entity everytime a batch is completed/published to avoid duplicate push of same values 
            entity_obj = Entity(entity_type, entity_id_prefix + ':Hexagon:' + str(hexagon_id)) # (re-)initialize the entity
            
        #'traffic_intensity_urb','traffic_occupation_urb', 'traffic_load_urb'    
        entity_obj.add_prop('traffic_intensity_urb', str(row['traffic_intensity_urb']) , observedAt= datetime.strptime(str(row.datetime), '%Y-%m-%d %H:%M:%S')) 
        entity_obj.add_prop('traffic_occupation_urb', str(row['traffic_occupation_urb']) , observedAt= datetime.strptime(str(row.datetime), '%Y-%m-%d %H:%M:%S')) 
        entity_obj.add_prop('traffic_load_urb', str(row['traffic_load_urb']) , observedAt= datetime.strptime(str(row.datetime), '%Y-%m-%d %H:%M:%S')) 
            
        if example_flag:
            print('INFO: Example data from the dataset to be published - as an example on how data looks.')
            print(entity_obj)
            example_flag=False

        if (counter %num_rows_batch_publish==0 or counter == len(hexagon_gdf)-1) and  counter!=0: # publish the batch data for entity to scorpio as the rows fulfill the number of rows needed for a batch, or the dataset has come to the end
            publish(entity_obj.to_dict()) # publish the entity
        counter+=1 
    print("... published data for hexagon: " +  str(hexagon_id))

ingestion_endtime= time.perf_counter() 
print('Info: Dataset is published successfully' 

      + ' --- It took ' + str(ingestion_endtime -ingestion_starttime) + ' seconds to publish the dataset with length ' + str(dataset_length))


