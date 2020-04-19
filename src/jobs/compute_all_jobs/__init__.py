from pyspark.sql.functions import *
import jobs.km_per_day_of_week,jobs.trip_per_day_of_week,jobs.trip_per_time_slice,jobs.trip_speed

def analyze(spark,df):
    res1 = jobs.trip_speed.analyze(spark,df)
    res2 = jobs.trip_per_day_of_week.analyze(spark,df)
    res3 = jobs.trip_per_time_slice.analyze(spark,df)
    res4 = jobs.km_per_day_of_week.analyze(spark,df)

    print('''Aperçu du nouveau dataframe avec la vitesse par trajet : {}\n
    Nombre de voyage par jour : {}\n
    Nombre de voyage par tranche horaire : {}\n
    Nombre de km par jour de la semaine : {}
    '''.format(res1,res2,res3,res4))

    return "files are saved"