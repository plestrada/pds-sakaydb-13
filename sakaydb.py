import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

class SakayDB():
    def __init__(self, data_dir):
        self.data_dir = data_dir
    
    def add_trip(self,
         driver,
         pickup_datetime,
         dropoff_datetime,
         passenger_count,
         pickup_loc_name,
         dropoff_loc_name,
         trip_distance,
         fare_amount):

    try:
        drv_check = driver.split(',')
        p_datetime = pd.to_datetime(pickup_datetime, format='%H:%M:%S,%d-%m-%Y')
        d_datetime = pd.to_datetime(dropoff_datetime, format='%H:%M:%S,%d-%m-%Y')
    except:
        raise SakayDBError
    
    if (~isinstance(passenger_count, int) or ~isinstance(pickup_loc_name, str) or ~isinstance(dropoff_loc_name, str)
        or ~isinstance(trip_distance, float) or ~isinstance(fare_amount, float)):
        raise SakayDBError
    
    drivers = pd.read_csv(f'{self.data_dir}/drivers.csv')
    driver = [x.strip() for x in driver.split(',')]
    driver_id = drivers[(drivers['last_name'].apply(lambda x: str(x).lower())==driver[0].lower()) &
                        (drivers['given_name'].apply(lambda x: str(x).lower())==driver[1].lower())]

    if len(driver_id)!=0:
        driver_id = ['driver_id'].iloc[0]
    else:
        driver_id = drivers['driver_id'].max() + 1
        new_driver = pd.DataFrame({'driver_id': [driver_id], 'last_name': [driver[0]], 'given_name': [driver[1]]})
        drivers = pd.concat([drivers, new_driver], axis=0)
        drivers.to_csv(f'{self.data_dir}/drivers.csv')

    pickup_loc_id = locations.loc[(locations['loc_name']==pickup_loc_name), 'location_id'].iloc[0]
    dropoff_loc_id = locations.loc[(locations['loc_name']==dropoff_loc_name), 'location_id'].iloc[0]

    try:
        trips = pd.read_csv(f'{self.data_dir}/trips.csv')
        trip_id = trips['trip_id'].max() + 1
        
        new_trip = pf.DataFrame({'trip_id': trip_id,
                      'driver_id': driver_id,
                      'pickup_datetime': pickup_datetime,
                      'dropoff_datetime': dropoff_datetime,
                    'passenger_count': passenger_count,
                      'pickup_loc_id': pickup_loc_id,
                      'dropoff_loc_id': dropoff_loc_id,
                    'trip_distance': trip_distance,
                      'fare_amount': fare_amount})
        
        sub_set = ['driver_id', 'pickup_datetime', 'dropoff_datetime', 'passenger_count', 'pickup_loc_id', 'dropoff_loc_id', 'trip_distance', 'fare_amount']
        dupe_trips = new_trip.isin({key:value.array for key, value in trips[sub_set].items()}).all(1).squeeze()
        
        if sum(dupe_trips) > 0:
            raise SakayDBError
        else:
            return trip_id

    except:
        trips = pf.DataFrame({'trip_id': 1,
                              'driver_id': driver_id,
                              'pickup_datetime': pickup_datetime,
                              'dropoff_datetime': dropoff_datetime,
                            'passenger_count': passenger_count,
                              'pickup_loc_id': pickup_loc_id,
                              'dropoff_loc_id': dropoff_loc_id,
                            'trip_distance': trip_distance,
                              'fare_amount': fare_amount})

        trips.to_csv(f'{self.data_dir}/trips.csv')
        return trip_id

class SakayDBError(ValueError):
    def __init__(self, exception):
        super().__init__(exception)
