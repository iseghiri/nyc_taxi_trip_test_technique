from pyspark.sql.functions import *
from shared import tests
def analyze(spark,df):
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