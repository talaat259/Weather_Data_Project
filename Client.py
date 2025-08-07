from kafka import KafkaConsumer
import json
import snowflake.connector
from SNOWFLAKE_CONFIG import SNOWFLAKE_CONFIG







def insert_to_snowflake(data):
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    
    try:
        sql = """
        INSERT INTO weather_data (
            City, Latitude, Longitude, Weather_Description,
            Actual_Temp, Feels_Like_Temp, Min_Temp, Max_Temp,
            Humidity, Pressure, Visibi, Wind_Speed,
            Wind_Direction, Gust, Cloud_Percentage, Country,
            Sunrise_Time, Sunset_Time, Time_Zone
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql, (
            data['City'],
            data['Lat'],                 # → Latitude
            data['Lon'],                 # → Longitude
            data['Description'],         # → Weather_Description
            data['Actual_Temp'],
            data['Feels_Like'],          # → Feels_Like_Temp
            data['Min_Temp'],
            data['Max_Temp'],
            data['Humidity'],
            data['Pressure'],
            data['Visibility'],          # → Visibi
            data['Wind_Speed'],
            data['Wind_Direction'],
            data['gust'],                # → Gust
            data['Cloudiness'],          # → Cloud_Percentage
            data['Country'],
            data['Sunrise'],             # → Sunrise_Time
            data['Sunset'],              # → Sunset_Time
            data['Timezone']             # → Time_Zone
        ))

        conn.commit()
        print("✅ Inserted into Snowflake.")
        
    except Exception as e:
        print(f"❌ Snowflake Insert Error: {e}")
    finally:
        cursor.close()
        conn.close()


cconsumer = KafkaConsumer(
    'weather_data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='weather_data_group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)


for message in cconsumer:
    data = message.value

    print(f"Received data: {data}")
    insert_to_snowflake(data)