from pyspark.sql.functions import *
def analyze(spark,df):
    df_with_day_of_week = df \
        .withColumn("pickup_datetime",to_timestamp(col("pickup_datetime"))) \
        .withColumn("week_day_number", date_format(col("pickup_datetime"), "u")) \
        .withColumn("week_day", date_format(col("pickup_datetime"), "E")) \

    df_with_tip_per_day = df_with_day_of_week.rdd.map(lambda x : (x.week_day,1)) \
    .reduceByKey(lambda x,y: x + y)

    result = df_with_tip_per_day.collect()

    return result 