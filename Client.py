from kafka import KafkaConsumer
import json
import duckdb
import pyspark


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
        
        # Here you can process the data as needed, e.g., store it in a database
        # For example, using DuckDB or PySpark to store the data
        # duckdb.execute("INSERT INTO weather_data_table VALUES (?)", (data,))
        # Or use PySpark DataFrame to process and store the data