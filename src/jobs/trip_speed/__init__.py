from math import radians,sin,cos
from pyspark.sql.functions import *
def analyze(spark,df):
    """ this function creates and saves the dataframe with the column speed, corresponding to the mean speed of the trip
        First it computes the distance between the pickup and dropoff points with the haversine formula 
        Then it computes the speed with speed = distance/time
        :param spark: SparkSession created in main
        :param df: DataFrame created in le main from the input 
        :type arg1: pyspark.sql.session.SparkSession
        :type arg1: pyspark.sql.dataframe.DataFrame
        :return: returns the list of the new_dataframe.take(5) and save the entire new dataframe in outputs
        :rtype: list
    """

    # Radius of the Earth
    r = 6371 

    df_with_speed = df \
    .withColumn("a", pow(sin(radians(col("dropoff_latitude") - col("pickup_latitude")) / 2), 2) + cos(radians(col("pickup_latitude"))) * cos(radians(col("dropoff_latitude"))) * pow(sin(radians(col("dropoff_longitude") - col("pickup_longitude")) / 2), 2)) \
    .withColumn("distance", asin(sqrt(col("a"))) * 2 * r) \
    .withColumn("speed",col("distance") / col("trip_duration"))

    df_with_speed.write.save("outputs/df_with_speed")
    result = df_with_speed.take(5)
    return result