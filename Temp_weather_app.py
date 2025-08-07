from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg
from pyspark.sql.types import StructType, StringType, FloatType

spark = SparkSession.builder \
    .appName("AvgTempByCountry") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

schema = StructType() \
    .add("City", StringType()) \
    .add("Latitude", FloatType()) \
    .add("Longitude", FloatType()) \
    .add("Weather_Description", StringType()) \
    .add("Actual_Temp", FloatType()) \
    .add("Feels_Like_Temp", FloatType()) \
    .add("Min_Temp", FloatType()) \
    .add("Max_Temp", FloatType()) \
    .add("Humidity", FloatType()) \
    .add("Pressure", FloatType()) \
    .add("Visibi", FloatType()) \
    .add("Wind_Speed", FloatType()) \
    .add("Wind_Direction", FloatType()) \
    .add("Gust", FloatType()) \
    .add("Cloud_Percentage", FloatType()) \
    .add("Country", StringType()) \
    .add("Sunrise_Time", StringType()) \
    .add("Sunset_Time", StringType()) \
    .add("Time_Zone", StringType())

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather_data") \
    .load()

parsed_df = raw_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

agg_df = parsed_df.groupBy("Country").agg(
    avg("Actual_Temp").alias("Avg_Temp")
)
query = agg_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
