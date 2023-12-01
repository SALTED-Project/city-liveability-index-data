#!/usr/bin/env python
# coding: utf-8

# In[20]:


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


num_rows_batch_publish=5000


# In[16]:


magnitudes = ["co", "pm10", "pm2.5", "no2", "o3"] # to be used
gdf_property_list=["co", "pm10", "pm2.5", "no2",  "o3"]
entity_id_prefix =  "urn:ngsi-ld:AirQualityObserved:Berlin"
entity_type = 'AirQualityObserved'


# In[17]:


pub_df = {}
gdf_no2 = pd.read_pickle('./../cgarrido/berlin_data/air_pollution/gdf_with_hex_no2.pkl')
gdf_o3 = pd.read_pickle('./../cgarrido/berlin_data/air_pollution/gdf_with_hex_o3.pkl')
gdf_pm10 = pd.read_pickle('./../cgarrido/berlin_data/air_pollution/gdf_with_hex_pm10.pkl')
gdf_pm2_5 = pd.read_pickle('./../cgarrido/berlin_data/air_pollution/gdf_with_hex_pm2.5.pkl')
gdf_co = pd.read_pickle('./../cgarrido/berlin_data/air_pollution/gdf_with_hex_co.pkl')


# In[18]:


gdf_list= [gdf_no2,  gdf_o3, gdf_pm10, gdf_pm2_5, gdf_co]


# In[28]:


gdf_o3


# In[29]:


example_flag=False
for property_index, tmp_gdf in enumerate(gdf_list): # for each dataset 
    ingestion_starttime= time.perf_counter() 
    example_flag=True
    prop =gdf_property_list[property_index]

    hexagon_id_list = tmp_gdf['hexagon_id'].unique()
    print("Info: Unique hexagons for the dataset : "+ str(hexagon_id_list))
    feature_list= tmp_gdf.columns.tolist()
    dataset_length = len(tmp_gdf)
    for hexagon_id in hexagon_id_list:
        hexagon_gdf = tmp_gdf[tmp_gdf['hexagon_id']== hexagon_id]
        
        entity_obj = Entity(entity_type, entity_id_prefix + ':Hexagon:' + str(hexagon_id)) # (re-)initialize the entity

        counter =0
        for index, row in hexagon_gdf.iterrows():
            lat=0
            lon=0
            if counter %num_rows_batch_publish==0: # create a new clean entity everytime a batch is completed/published to avoid duplicate push of same values 
                entity_obj = Entity(entity_type, entity_id_prefix + ':Hexagon:' + str(hexagon_id)) # (re-)initialize the entity
            for f in range(len(feature_list)):
                if feature_list[f] == 'lat': lat = row[f]
                if feature_list[f] == 'lon': lon=row[f]
                if lat !=0 and lon !=0: # both values are obtained for location, add the location attribute 
                    location_value = Point((lat, lon))
                    entity_obj.add_geoprop("location", location_value, observedAt= datetime.strptime(str(row.datetime), '%d.%m.%Y %H:%M')) # MAKE THIS A POINT (SIVA EXAMPLE)
                elif feature_list[f] in ["co", "pm10", "pm2.5", "no2", "o3"]:
                    if feature_list[f] == "pm2.5":
                        print(feature_list[f], row[f])
                        entity_obj.add_prop('pm2_5', str(row[f]) , observedAt= datetime.strptime(str(row.datetime), '%d.%m.%Y %H:%M'))                 
                    else:
                        print(feature_list[f], row[f])
                        entity_obj.add_prop(prop, str(row[f]) , observedAt= datetime.strptime(str(row.datetime), '%d.%m.%Y %H:%M'))                 

                #elif(feature_list[f] not in ['datetime','type']):
                #    entity_obj.add_prop(feature_list[f], str(row[f]) , observedAt= datetime.strptime(str(row.datetime), '%Y-%m-%d %H:%M:%S'))                 
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


# In[ ]:




