from math import radians,sin,cos
from pyspark.sql.functions import *

def analyze(spark,df):
    
    df = spark \
    .read \
    .format("csv") \
    .option("header", "true") \
    .load("data/train.csv")

    # Radius of the Earth
    r = 6371 

    df_with_speed = df \
    .withColumn("a", pow(sin(radians(col("dropoff_latitude") - col("pickup_latitude")) / 2), 2) + cos(radians(col("pickup_latitude"))) * cos(radians(col("dropoff_latitude"))) * pow(sin(radians(col("dropoff_longitude") - col("pickup_longitude")) / 2), 2)) \
    .withColumn("distance", asin(sqrt(col("a"))) * 2 * r) \
    .withColumn("speed",col("distance") / col("trip_duration"))

    result = df_with_speed.take(5)
    return result