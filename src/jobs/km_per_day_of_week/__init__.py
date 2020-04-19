from pyspark.sql.functions import *
from shared import tests

def analyze(spark,df):

    r = 6371

    df_with_distance_day_of_week = df \
        .withColumn("a", pow(sin(radians(col("dropoff_latitude") - col("pickup_latitude")) / 2), 2) + cos(radians(col("pickup_latitude"))) * cos(radians(col("dropoff_latitude"))) * pow(sin(radians(col("dropoff_longitude") - col("pickup_longitude")) / 2), 2)) \
        .withColumn("distance", asin(sqrt(col("a"))) * 2 * r) \
        .withColumn("pickup_datetime",to_timestamp(col("pickup_datetime"))) \
        .withColumn("week_day_number", date_format(col("pickup_datetime"), "u")) \
        .withColumn("week_day", date_format(col("pickup_datetime"), "E")) \

    df_with_km_per_day_of_week = df_with_distance_day_of_week.rdd \
        .map(lambda x : (x.week_day,x.distance)) \
        .reduceByKey(lambda x,y: x + y)


    result = df_with_km_per_day_of_week.collect()

    try:
        tests.check_week_days(result)
    except AssertionError as msg:
        print(msg)
    
    ## Even if the exception is raised we save the file, it may be only one or two bad date format 
    ## The exception is here to alert and make the team check the input data 
    df_with_km_per_day_of_week.toDF().write.save("outputs/df_with_km_per_day_of_week")

    

    return result 