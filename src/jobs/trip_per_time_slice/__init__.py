from pyspark.sql.functions import *
#on divisera les tranche horaire en fonction de l'heure de départ du taxi : 
#- 00:00 - 04:00 -> tranche horaire 1
#- 04:00 - 08:00 -> tranche horaire 2 
#- 08:00 - 12:00 -> tranche horaire 3
#- 12:00 - 16:00 -> tranche horaire 4
#- 16:00 - 20:00 -> tranche horaire 5
#- 20:00 - 23:59 -> tranche horaire 6

def analyze(spark,df):
    """ this function creates and saves the dataframe with the number of trips per time_slice

        :param spark: SparkSession created in main
        :param df: DataFrame created in le main from the input 
        :type arg1: pyspark.sql.session.SparkSession
        :type arg1: pyspark.sql.dataframe.DataFrame
        :return: returns the list of the new_dataframe.collect() and save the new dataframe in outputs
        :rtype: list
    """

    df_with_time_slice = df \
        .withColumn("pickup_datetime",to_timestamp(col("pickup_datetime"))) \
        .withColumn("hour_pickup", date_format(col("pickup_datetime"), "hh:mm")) \

    time_slice_column = when(("00:00"<=col("hour_pickup")) & (col("hour_pickup")<"04:00"), "slice_1") \
        .when(("04:00"<=col("hour_pickup")) & (col("hour_pickup")<"08:00"), "slice_2")\
        .when(("08:00"<=col("hour_pickup")) & (col("hour_pickup")<"12:00"), "slice_3")\
        .when(("12:00"<=col("hour_pickup")) & (col("hour_pickup")<"16:00"), "slice_4")\
        .when(("16:00"<=col("hour_pickup")) & (col("hour_pickup")<"20:00"), "slice_5")\
        .when(("20:00"<=col("hour_pickup")) & (col("hour_pickup")<="23:59"), "slice_6")\
        .otherwise("slice_unknown")

    df_with_time_slice = df_with_time_slice.withColumn("time_slice",time_slice_column)

    df_with_trajet_per_time_slice = df_with_time_slice.rdd \
        .map(lambda x : (x.time_slice,1)) \
        .reduceByKey(lambda x,y: x + y)

    result = df_with_trajet_per_time_slice.collect()
    df_with_trajet_per_time_slice.toDF().write.save("outputs/df_with_trajet_per_time_slice")

    return result 