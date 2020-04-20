from pyspark.sql.functions import *
from shared import tests

def analyze(spark,df):
    """ this function creates and saves the dataframe with the km per week day

        :param spark: SparkSession created in main
        :param df: DataFrame created in main from the input 
        :type arg1: pyspark.sql.session.SparkSession
        :type arg1: pyspark.sql.dataframe.DataFrame
        :return: returns the list of the new_dataframe.collect() and save the new dataframe in outputs
        :rtype: list
    """
    #Earth radius
    r = 6371

    df_with_distance_day_of_week = df \
        .withColumn("a", pow(sin(radians(col("dropoff_latitude") - col("pickup_latitude")) / 2), 2) + cos(radians(col("pickup_latitude"))) * cos(radians(col("dropoff_latitude"))) * pow(sin(radians(col("dropoff_longitude") - col("pickup_longitude")) / 2), 2)) \
        .withColumn("distance", asin(sqrt(col("a"))) * 2 * r) \
        .withColumn("pickup_datetime",to_timestamp(col("pickup_datetime"))) \
        .withColumn("week_day_number", date_format(col("pickup_datetime"), "u")) \
        .withColumn("week_day", date_format(col("pickup_datetime"), "E"))

    #we map each day week with the corresponding distance, then we reduce by the day key with the sum operator 
    df_with_km_per_day_of_week = df_with_distance_day_of_week.rdd \
        .map(lambda x : (x.week_day,x.distance)) \
        .reduceByKey(lambda x,y: x + y)

    #we can use collect since we know there will be at most 8 row (week days + unknown if the format is not correct)
    result = df_with_km_per_day_of_week.collect()

    #check that the pickup_datetime format has not change 
    try:
        tests.check_week_days(result)
    except AssertionError as msg:
        print(msg)
    
    ## Even if the exception is raised we save the file, it may be only one or two bad date format 
    ## The exception is here to alert and make the team check the input data 
    df_with_km_per_day_of_week.toDF().write.save("outputs/df_with_km_per_day_of_week")

    

    return result 