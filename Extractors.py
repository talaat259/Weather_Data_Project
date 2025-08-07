import requests
import time
import datetime
import json
import csv
from kafka import KafkaProducer



# Produce the data to Kafka
def produce_to_kafka(data):
        producer = KafkaProducer(bootstrap_servers='localhost:9092',
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        producer.send('weather_data', value=data)
        producer.flush()
        print("Data sent to Kafka topic 'weather_data'")


        
def starter():
    API_KEY='d9ac0d1ba61ab167785979b6f42a2a92'
    with open('european_cities_lat_lon.csv', 'r') as file:
        reader = csv.DictReader(file)
        cities = [row for row in reader]

    #print(cities)
    for elem in cities:
        city=elem["City"]
        lat=elem["Latitude"]
        print(lat)
        lon=elem["Longitude"]
        print(lon)
        URL=f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=metric'
        cityURL=(city,URL)
        print(f"Fetching weather data for {city}...")
        time.sleep(1)  # To avoid hitting the API rate limit
        result=get_weather_data(cityURL)
        #print(result)

def get_weather_data(cityURL):
    try:
        response = requests.get(cityURL[1])
        response.raise_for_status()  # Raise an error for bad responses
        data = response.json()
        description(data, cityURL[0])  # Call description with the data and city name
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data: {e}")
        return None

#res=get_weather_data(URL)

def description(data,CITY):
    Data_Return={}
    Data_Return['City'] = CITY
    coordinates=data.get('coord', None)
    Data_Return['Lat'] = coordinates.get('lat',None)
    Data_Return['Lon'] = coordinates.get('lon',None)

    weather=data.get('weather', [None])[0]
    Data_Return['Description'] = weather.get('description',None)

    
    Temp = data.get('main', None)
    Data_Return['Actual_Temp'] = Temp.get('temp',None)
    Data_Return['Feels_Like'] = Temp.get('feels_like',None)
    Data_Return['Min_Temp'] = Temp.get('temp_min',None )
    Data_Return['Max_Temp'] = Temp.get('temp_max',None)
    Data_Return['Humidity'] = Temp.get('humidity',None)


    pressure_hpa = Temp.get('pressure')  # Returns None if not found

    if pressure_hpa is not None:
        Data_Return['Pressure'] = pressure_hpa * 100  # Convert to Pa
    else:
        #Data_Return['Pressure'] = 'No pressure data available'
        pass


    Data_Return['Visibility'] = data.get('visibility',None)

    wind_conditions = data.get('wind', None)
    Data_Return['Wind_Speed'] = wind_conditions.get('speed',None)
    Data_Return['Wind_Direction'] = wind_conditions.get('deg',None)  
    #Data_Return['Cloudiness'] = data.get('clouds', None).get('all', 'No cloudiness data available')
    Data_Return['gust']=wind_conditions.get('gust',None)



    clouds = data.get('clouds', None).get('all', None)  # Use .get() to avoid KeyError if 'clouds' is not present
    #Data_Return['Cloudiness'] = clouds.get('all', 'No cloudiness data available')
    if clouds is not None:
        Data_Return['Cloudiness'] = float(clouds)/100 #0.xx% 
    else:
        pass #Data_Return['Cloudiness'] = 'No Cloud data available'
    

    sys= data.get('sys', None)
    Data_Return['Country'] = sys.get('country',None)
    Sunrise = sys.get('sunrise')
    if Sunrise is not None:
        Data_Return['Sunrise'] = datetime.datetime.fromtimestamp(Sunrise).strftime('%Y-%m-%d %H:%M:%S')
    else:
        #Data_Return['Sunrise'] = 'No sunrise data available'
        pass

    Sunset = sys.get('sunset')
    if Sunset is not None:
        Data_Return['Sunset'] = datetime.datetime.fromtimestamp(Sunset).strftime('%Y-%m-%d %H:%M:%S')
    else:
        pass #Data_Return['Sunset'] = 'No sunset data available'

    timezone = data.get('timezone', 0)
    if timezone is not None:
        offset_hours = timezone // 3600
        offset_minutes = (abs(timezone) % 3600) // 60
        sign = '+' if timezone >= 0 else '-'
        Data_Return['Timezone'] = f'UTC{sign}{abs(offset_hours):02}:{offset_minutes:02}'
    else:
        pass   #Data_Return['Timezone'] = 'No timezone data available'
    #jason_data = json.dumps(Data_Return, indent=4)
    #return jason_data
    print(Data_Return) 
    produce_to_kafka(Data_Return)  # Call the function to produce data to Kafka


    

if __name__ == "__main__":
    cityURL=starter()
   

    
""" res=get_weather_data(URL)
print(res)
print(type(res))
des=description(res)

print("--"*50)
print(des)
print(type(des)) """