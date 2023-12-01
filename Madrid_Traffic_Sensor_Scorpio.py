#!/usr/bin/env python
# coding: utf-8

# In[11]:


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



num_rows_batch_publish=5000 # number of objects to be batch published to Scorpio


# # Notes:
# Consider every hexagon as an entity, and sensor data as properties

import pandas as pd
import geopandas as gpd

from geojson.geometry import Point, Polygon, MultiLineString


traffic_sensor_df = pd.read_pickle('./../cgarrido/madrid_data/traffic/gdf_proc_traffic_month_1.pkl')


#gdf_list= [gdf_PM2_5,gdf_NO2, gdf_O3, gdf_PM10,  gdf_SO2, gdf_CO]
gdf_property_list=["ttype","intensity","occupation","load","avg_speed"]
entity_id_prefix =  "urn:ngsi-ld:TrafficObserved:Madrid:"
entity_type = 'TrafficObserved'


# In[15]:


from pyproj import Transformer

transformer = Transformer.from_crs("EPSG:3857", "EPSG:4326") # transform geometry to lat/lon
poly = traffic_sensor_df['geometry']
#traffic_sensor_df['lat'], traffic_sensor_df['lon'] = transformer.transform(poly.centroid.x,poly.centroid.y) 
#for g in gdf_list:
#    poly = g['geometry']
#    g['lat'] ,  g['lon'] =  transformer.transform(poly.centroid.x,poly.centroid.y) 


# In[16]:


traffic_sensor_df.groupby(by = ['id','lat', 'lon']).count()


# In[ ]:


for mnth in range(1,13):
    ingestion_starttime= time.perf_counter() 
    traffic_sensor_df = pd.read_pickle('./../cgarrido/madrid_data/traffic/gdf_proc_traffic_month_'+str(mnth)+'.pkl')
    poly = traffic_sensor_df['geometry']
    traffic_sensor_df['lat'], traffic_sensor_df['lon'] = transformer.transform(poly.centroid.x,poly.centroid.y) 
    example_flag=True
    uniqueStationIDs= traffic_sensor_df['id'].unique()
    dataset_length= len(traffic_sensor_df)
    feature_list= traffic_sensor_df.columns.tolist()
    sensor_name = 'Traffic_month_'+str(mnth)
    for u in uniqueStationIDs:
        station_tmp_gdf = traffic_sensor_df[traffic_sensor_df['id'] == u] 
        entity_obj = Entity(entity_type, entity_id_prefix + sensor_name + ":ID:" + str(int(u)) )
        counter=0
        for index, row in station_tmp_gdf.iterrows():
            lat=0
            lon=0
            if counter %num_rows_batch_publish==0: # create a new clean entity everytime a batch is completed/published to avoid duplicate push of same values 
                entity_obj = Entity(entity_type, entity_id_prefix + sensor_name + ":ID:" + str(int(u))) # (re-)initialize the entity
            for f in range(len(feature_list)):
                if feature_list[f] == 'lat': lat = row[f]
                if feature_list[f] == 'lon': lon=row[f]
                if lat !=0 and lon !=0: # both values are obtained for location, add the location attribute 
                    location_value = Point((lat, lon))
                    entity_obj.add_geoprop("location", location_value, observedAt= datetime.strptime(str(row.datetime), '%Y-%m-%d %H:%M:%S')) # MAKE THIS A POINT (SIVA EXAMPLE)
                elif feature_list[f] in ['intensity','occupation','load','avg_speed']:
                    entity_obj.add_prop(feature_list[f], str(row[f]) , observedAt= datetime.strptime(str(row.datetime), '%Y-%m-%d %H:%M:%S'))                 
                #elif(feature_list[f] not in ['datetime','type']):
                #    entity_obj.add_prop(feature_list[f], str(row[f]) , observedAt= datetime.strptime(str(row.datetime), '%Y-%m-%d %H:%M:%S'))                 
            if example_flag:
                print('INFO: Example data from the dataset to be published - as an example on how data looks.')
                print(entity_obj)
                example_flag=False

            if (counter %num_rows_batch_publish==0 or counter == len(station_tmp_gdf)-1) and  counter!=0: # publish the batch data for entity to scorpio as the rows fulfill the number of rows needed for a batch, or the dataset has come to the end
                publish(entity_obj.to_dict()) # publish the entity
                print('Published one batch')
            counter+=1 
    print("... published data for dataset: " +  sensor_name)

    ingestion_endtime= time.perf_counter() 
    print('Info: Dataset is published successfully' 

    + ' --- It took ' + str(ingestion_endtime -ingestion_starttime) + ' seconds to publish the dataset with length ' + str(dataset_length))

    

