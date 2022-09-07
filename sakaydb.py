import numpy as np
import pandas as pd
import numpy as np
import os.path
from pathlib import Path
import matplotlib.pyplot as plt


class SakayDB():
    def __init__(self, data_dir):
        self.data_dir = data_dir

    # BJ - add_trip
    def check_create_file(self, filename, columns):
        if not os.path.exists(filename):
            with open(filename, mode='w', encoding='utf-8') as f:
                f.write(','.join(columns) + '\n')

    def get_driver_id(self, driver):
        last_name, given_name = [x.strip() for x in driver.split(',')]

        fn = f'{self.data_dir}/drivers.csv'
        df_drivers = pd.read_csv(fn)
        cond = ((df_drivers['given_name'].str.lower() == given_name.lower()) &
                (df_drivers['last_name'].str.lower() == last_name.lower()))

        if cond.any():
            driver_id = df_drivers[cond]['driver_id'].item()
            return driver_id
        else:
            return None

    def get_loc_id(self, location):
        fn = f'{self.data_dir}/locations.csv'
        df_locations = pd.read_csv(fn)
        cond = (df_locations['loc_name'].str.lower()
                == location.lower().strip())

        if cond.any():
            loc_id = df_locations[cond]['location_id'].item()
            return loc_id
        else:
            return None

    def get_trip_id(self, driver, pickup_datetime, dropoff_datetime, passenger_count,
                    pickup_loc_name, dropoff_loc_name, trip_distance, fare_amount):
        fn = f'{self.data_dir}/trips.csv'
        df_trips = pd.read_csv(fn)
        cond = ((df_trips['driver_id'] == self.get_driver_id(driver)) &
                (df_trips['pickup_datetime'] == pickup_datetime) &
                (df_trips['dropoff_datetime'] == dropoff_datetime) &
                (df_trips['passenger_count'] == passenger_count) &
                (df_trips['pickup_loc_id'] == self.get_loc_id(pickup_loc_name)) &
                (df_trips['dropoff_loc_id'] == self.get_loc_id(dropoff_loc_name)) &
                (df_trips['trip_distance'] == trip_distance) &
                (df_trips['fare_amount'] == fare_amount))

        if cond.any():
            trip_id = df_trips[cond]['trip_id'].item()
            return trip_id
        else:
            return None

    def add_driver(self, driver):
        last_name, given_name = [x.strip() for x in driver.split(',')]

        fn = f'{self.data_dir}/drivers.csv'
        cols = ['driver_id', 'given_name', 'last_name']

        self.check_create_file(fn, cols)

        df_drivers = pd.read_csv(fn)

        if pd.isnull(df_drivers['driver_id'].max()):
            driver_id = 1
        else:
            driver_id = df_drivers['driver_id'].max() + 1

        if self.get_driver_id(driver) is None:
            data = {
                'driver_id': [driver_id],
                'given_name': [given_name],
                'last_name': [last_name]
            }
            pd.DataFrame(data).to_csv(fn, mode='a', index=False, header=False)

    def add_trip(self, driver, pickup_datetime, dropoff_datetime, passenger_count,
                 pickup_loc_name, dropoff_loc_name, trip_distance, fare_amount):

        try:
            drv_check = driver.split(',')
            p_datetime = pd.to_datetime(pickup_datetime, format='%H:%M:%S,%d-%m-%Y')
            d_datetime = pd.to_datetime(dropoff_datetime, format='%H:%M:%S,%d-%m-%Y')
            trip_distance = float(trip_distance)
            fare_amount = float(fare_amount)

        except:
            raise SakayDBError('Wrong input format.')

        if isinstance(pickup_loc_name, str) and isinstance(dropoff_loc_name, str) and isinstance(passenger_count, int):
            pass
        else:
            raise SakayDBError('Wrong input format.')

        fn = f'{self.data_dir}/trips.csv'
        cols = ['trip_id', 'driver_id', 'pickup_datetime',
                'dropoff_datetime', 'passenger_count', 'pickup_loc_id',
                'dropoff_loc_id', 'trip_distance', 'fare_amount']

        self.check_create_file(fn, cols)

        df_trips = pd.read_csv(fn)

        self.add_driver(driver)

        if pd.isnull(df_trips['trip_id'].max()):
            trip_id = 1
        else:
            trip_id = df_trips['trip_id'].max() + 1

        if self.get_trip_id(driver, pickup_datetime, dropoff_datetime, passenger_count,
                            pickup_loc_name, dropoff_loc_name, trip_distance, fare_amount) is None:
            trip_data = {
                'trip_id': [trip_id],
                'driver_id': [self.get_driver_id(driver)],
                'pickup_datetime': pickup_datetime,
                'dropoff_datetime': dropoff_datetime,
                'passenger_count': [passenger_count],
                'pickup_loc_id': [self.get_loc_id(pickup_loc_name)],
                'dropoff_loc_id': [self.get_loc_id(dropoff_loc_name)],
                'trip_distance': [trip_distance],
                'fare_amount': [fare_amount]
            }

            pd.DataFrame(trip_data).to_csv(fn, encoding='utf-8',
                                           mode='a', index=False, header=False)
            return trip_id
        else:
            raise SakayDBError('Trip exists in the database')

    # Pat - add_trips
    def add_trips(self, trips_list):
        trip_ids = []
        for i, row in enumerate(trips_list):
            try:
                driver = row['driver']
                pickup_datetime = row['pickup_datetime']
                dropoff_datetime = row['dropoff_datetime']
                passenger_count = row['passenger_count']
                pickup_loc_name = row['pickup_loc_name']
                dropoff_loc_name = row['dropoff_loc_name']
                trip_distance = row['trip_distance']
                fare_amount = row['fare_amount']
                
                try:
                    trip_id = self.add_trip(driver, pickup_datetime, dropoff_datetime, passenger_count,
                                  pickup_loc_name, dropoff_loc_name, trip_distance, fare_amount)
                    trip_ids.append(trip_id)
                except:
                    print(f'Warning: trip index {i} is already in the database. Skipping...')    
            
            except:
                print(f'Warning: trip index {i} has invalid or incomplete information. Skipping...')
        
        return trip_ids

class SakayDBError(ValueError):
    def __init__(self, exception):
        super().__init__(exception)
