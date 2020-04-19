def check_input_schema(df):
    expected_cols = ['id',
        'vendor_id',
        'pickup_datetime',
        'dropoff_datetime',
        'passenger_count',
        'pickup_longitude',
        'pickup_latitude',
        'dropoff_longitude',
        'dropoff_latitude',
        'store_and_fwd_flag',
        'trip_duration']
    
    df_cols = [df.schema[i].name for i in range(len(df.schema))]
    #check if all the needed columns are there. If there is more it's not a problem
    #we could add another check to send a message if a new column is added
    assert set(expected_cols).issubset(df_cols), "The schema is not the one expected, one (or more) of the expected columns is (are) missing"

def check_week_days(df_collect):
    expected_days= ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
    days = [key_value[0] for key_value in df_collect]
    #the "to_timestamp" methode used to obtain week days returns "None" is the date format is not correct
    #Thus the "days" list will contain "None" contrary to the "expected_days" list
    assert set(expected_days) == set(days), "Check the pickup_datetime column, it might have change"