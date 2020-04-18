from pyspark.sql.functions import *
import jobs.km_per_day_of_week,jobs.trip_per_day_of_week,jobs.trip_per_time_slice,jobs.trip_speed

def analyze(spark,df):
    res1 = jobs.trip_speed.analyze(spark,df)
    res2 = jobs.trip_per_day_of_week.analyze(spark,df)
    res3 = jobs.trip_per_time_slice.analyze(spark,df)
    res4 = jobs.km_per_day_of_week.analyze(spark,df)

    res1.write.format("csv").option("header","true").save("data/trip_speed_take5.csv")
    res2.write.format("csv").option("header","true").save("data/trip_per_day_of_week.csv")
    res3.write.format("csv").option("header","true").save("data/trip_per_time_slice.csv")
    res4.write.format("csv").option("header","true").save("data/km_per_day_of_week.csv")

    return "csv files are saved"