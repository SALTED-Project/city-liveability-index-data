# city-liveability-index-data
The code for the data that are visualized in the City Liveability Index Flexible Frontend. 

In summary, the code includes the Python scripts for the following for the two cities Madrid and Berlin (example cities). The code can be replicated for other cities with small modifications.
- Sensor data from Madrid and Berlin that are published as NGSI-LD entities
- Data are mapped to hexagons from Madrid and Berlin and the hexagonal data is published as NGSI-LD entities.
- Data are mapped to indices and sub-indices related to air quality and published as NGSI-LD entities.
- Building data that are from various cities all over Europe (from [Eubocco] https://eubucco.com/), also published as NGSI-LD entities.

The enrichment includes the following aspects:
•	Sensor entities: The mapping of sensor data as “sensor entities” to the NGSI-LD for sharing with the Scorpio Broker and the user interfaces. Each sensor is considered as a separate NGSI-LD entity.
•	Hexagon pre-processed entities: The mapping of the data from sensors to the relevant hexagons that represent smaller regions in a city. Each hexagon may have various sensor data available. The hexagons are represented as NGSI-LD entities. The preprocessing includes filtering, data cleaning, and interpolation.
•	City liveability index applied on hexagons: Applying of the city liveability indices to calculate measurable KPIs for the given hexagon area. This step includes the calculation of the indices from the openly accepted standards. Namely, the Sustainable Development Goals-related indices by the SDG portal  that can be calculated by the sensor data are chosen. 
•	City liveability index applied on cities: The mapping of the data from hexagons to the entire city based on the indices. This step includes simple aggregation of the indices from the hexagon-level to the entire city. 


Implemented indices: Air quality indices for NO2, CO, CO2, O3, PM2.5, PM10

The city liveability indices are calculated as a proof-of-concept to showcase the idea. The air quality-related index called "Jahresmittelwert Feinstaub (PM₁₀) je Gebietseinheit" related to UN SDG goals is used and similar indices are calculated for annual air pollution (daily moving average) from Madrid and Berlin cities. The index is used as initially defined from the website SDG-Portal https://sdg-portal.de/en/. 


Dataset name	

City - Description

- Madrid Air Quality	NO2/CO/O3/PM2.5/PM10 from stations in Madrid every 15 minutes	
- Madrid Traffic	Street-wide density every 15 min	
- Madrid Noise	Noise from different stations	
- Madrid Weather	Temp., wind speed/direction, pressure, RH, irradiation, rain	
- Berlin Weather	Hourly temperature, rain, wind speed/direction, pressure	
- Berlin Air Quality	PM10, PM2.5, NO2, O3,  CO	
- Berlin Traffic	Street-wide hourly vehicle counts with different vehicle types	
- Berlin, Madrid City Building Data	Building height/type dataset	

List of indices

'urn:ngsi-ld:CityLiveabilityIndex:Madrid:Index:AirQualityAnnualMean:CO',
 'urn:ngsi-ld:CityLiveabilityIndex:Madrid:Index:AirQualityAnnualMean:PM10',
 'urn:ngsi-ld:CityLiveabilityIndex:Madrid:Index:AirQualityAnnualMean:PM2_5',
 'urn:ngsi-ld:CityLiveabilityIndex:Madrid:Index:AirQualityAnnualMean:NO2',
 'urn:ngsi-ld:CityLiveabilityIndex:Madrid:Index:AirQualityAnnualMean:O3'

 'urn:ngsi-ld:CityLiveabilityIndex:Berlin:Index:AirQualityAnnualMean:CO',
 'urn:ngsi-ld:CityLiveabilityIndex:Berlin:Index:AirQualityAnnualMean:PM10',
 'urn:ngsi-ld:CityLiveabilityIndex:Berlin:Index:AirQualityAnnualMean:PM2_5',
 'urn:ngsi-ld:CityLiveabilityIndex:Berlin:Index:AirQualityAnnualMean:NO2',
 'urn:ngsi-ld:CityLiveabilityIndex:Berlin:Index:AirQualityAnnualMean:O3'

List of sub indices

 urn:ngsi-ld:CityLiveabilityIndex:Madrid:SubIndex:AirQualityAnnualMean:CO
  'urn:ngsi-ld:CityLiveabilityIndex:Madrid:SubIndex:AirQualityAnnualMean:PM10',
 'urn:ngsi-ld:CityLiveabilityIndex:Madrid:SubIndex:AirQualityAnnualMean:PM2_5',
 'urn:ngsi-ld:CityLiveabilityIndex:Madrid:SubIndex:AirQualityAnnualMean:NO2',
 'urn:ngsi-ld:CityLiveabilityIndex:Madrid:SubIndex:AirQualityAnnualMean:O3'

 'urn:ngsi-ld:CityLiveabilityIndex:Berlin:SubIndex:AirQualityAnnualMean:CO',
 'urn:ngsi-ld:CityLiveabilityIndex:Berlin:SubIndex:AirQualityAnnualMean:PM10',
 'urn:ngsi-ld:CityLiveabilityIndex:Berlin:SubIndex:AirQualityAnnualMean:PM2_5',
 'urn:ngsi-ld:CityLiveabilityIndex:Berlin:SubIndex:AirQualityAnnualMean:NO2',
 'urn:ngsi-ld:CityLiveabilityIndex:Berlin:SubIndex:AirQualityAnnualMean:O3'


 Future improvements for some of the code: 
For some existing problems in the code, we fixed those either on the latest code or on the Scorpio Broker side, but for the future code usage the below items may help fix those early on.

- While creating an Entity using the "Entity()" constructor of the NGSI-LD library, make sure to add a third argument (if not there already).
- The third argument should be the context link for the smart data models (The initial two arguments correspond to entity type and entity id).
- In the existing code, lots of different property values are included as string, while most of them were integers, this is a mistake. 
- In the loops, change the string conversion of the property value and send the value as is (e.g., integer, float, or string).
- For every entity with a location, we must add a "location" property.
- If the entity is a polygon, the location of it should be a polygon (not simple coordinate)
- Coordinates should be lon, lat order (e.g., [20.12, 12.32] corresponds to 20.12 lon and 12.32 lat.
- While creating an Entity using the "Entity()" constructor of the NGSI-LD library, make sure to add a third argument (if not there already).
