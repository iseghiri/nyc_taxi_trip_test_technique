from pyspark.sql.functions import *
from shared import tests
def analyze(spark,df):
    """ this function creates and saves the dataframe with the number of trips per week day

        :param spark: SparkSession created in main
        :param df: DataFrame created in main from the input 
        :type arg1: pyspark.sql.session.SparkSession
        :type arg1: pyspark.sql.dataframe.DataFrame
        :return: returns the list of the new_dataframe.collect() and save the new dataframe in outputs
        :rtype: list
    """
    df_with_day_of_week = df \
        .withColumn("pickup_datetime",to_timestamp(col("pickup_datetime"))) \
        .withColumn("week_day_number", date_format(col("pickup_datetime"), "u")) \
        .withColumn("week_day", date_format(col("pickup_datetime"), "E")) \

    df_with_tip_per_day = df_with_day_of_week.rdd \
        .map(lambda x : (x.week_day,1)) \
        .reduceByKey(lambda x,y: x + y)

    result = df_with_tip_per_day.collect()
    try:
        tests.check_week_days(result)
    except AssertionError as msg:
        print(msg)
    
    ## Even if the exception is raised we save the file, it may be only one or two bad date format 
    ## The exception is here to alert and make the team check the input data 
    df_with_tip_per_day.toDF().write.save("outputs/df_with_tip_per_day")

    return result 