from pyspark.sql.functions import *
import jobs.km_per_day_of_week,jobs.trip_per_day_of_week,jobs.trip_per_time_slice,jobs.trip_speed

def analyze(spark,df):
    """ this function calls all the analyse function of each job

        :param spark: SparkSession created in main
        :param df: DataFrame created in main from the input 
        :type arg1: pyspark.sql.session.SparkSession
        :type arg1: pyspark.sql.dataframe.DataFrame
        :return: only return a message. Yet it saves the files in the outputs directory
        :rtype: string
    """

    res1 = jobs.trip_speed.analyze(spark,df)
    res2 = jobs.trip_per_day_of_week.analyze(spark,df)
    res3 = jobs.trip_per_time_slice.analyze(spark,df)
    res4 = jobs.km_per_day_of_week.analyze(spark,df)

    print('''Overview of the new dataframe with speed per trip : {}\n
    Number of trips per week day : {}\n
    Number of trips per time_slice : {}\n
    Number of km per week day : {}
    '''.format(res1,res2,res3,res4))

    return "files are saved"