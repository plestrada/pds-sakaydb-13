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
            trip_distance = float(trip_distance)
            fare_amount = float(fare_amount)

        except:
            raise SakayDBError('There is a wrong input format.')

        if isinstance(pickup_loc_name, str) and isinstance(dropoff_loc_name, str) and isinstance(passenger_count, int):
            pass
        else:
            raise SakayDBError('There is a wrong input format.')

        try:
            drivers = pd.read_csv(f'{self.data_dir}/drivers.csv')
        except:
            drivers = pd.DataFrame({'driver_id': [np.nan], 'last_name': [np.nan],
                                    'given_name': [np.nan]})

        driver = [x.strip() for x in driver.split(',')]
        driver_id = drivers[(drivers['last_name'].apply(lambda x: str(x).lower()) == driver[0].lower()) &
                            (drivers['given_name'].apply(lambda x: str(x).lower()) == driver[1].lower())]

        if len(driver_id) != 0:
            driver_id = drivers['driver_id'].iloc[0]
        else:
            driver_id = drivers['driver_id'].max(
            ) + 1 if str(drivers['driver_id'].max()) != 'nan' else 1
            new_driver = pd.DataFrame({'driver_id': [driver_id], 'last_name': [driver[0]],
                                       'given_name': [driver[1]]})
            drivers = pd.concat([drivers, new_driver], axis=0)
            drivers.dropna(how='all', axis=0, inplace=True)
            drivers.to_csv(f'{self.data_dir}/drivers.csv', index=False)

        locations = pd.read_csv(f'{self.data_dir}/locations.csv')
        pickup_loc_id = locations.loc[(locations['loc_name'] == pickup_loc_name), 'location_id'].iloc[0]
        dropoff_loc_id = locations.loc[(locations['loc_name'] == dropoff_loc_name), 'location_id'].iloc[0]

        try:
            trips = pd.read_csv(f'{self.data_dir}/trips.csv')
            trip_id = trips['trip_id'].max() + 1

        except:
            trips = pd.DataFrame({'trip_id': np.nan,
                                  'driver_id': np.nan,
                                  'pickup_datetime': np.nan,
                                  'dropoff_datetime': np.nan,
                                  'passenger_count': np.nan,
                                  'pickup_loc_id': np.nan,
                                  'dropoff_loc_id': np.nan,
                                  'trip_distance': np.nan,
                                  'fare_amount': np.nan}, index=[0])

            trip_id = 1

        new_trip = pd.DataFrame({'trip_id': trip_id,
                                 'driver_id': driver_id,
                                 'pickup_datetime': pickup_datetime,
                                 'dropoff_datetime': dropoff_datetime,
                                 'passenger_count': passenger_count,
                                 'pickup_loc_id': pickup_loc_id,
                                 'dropoff_loc_id': dropoff_loc_id,
                                 'trip_distance': trip_distance,
                                 'fare_amount': fare_amount}, index=[0])

        sub_set = ['driver_id', 'pickup_datetime', 'dropoff_datetime', 'passenger_count',
                   'pickup_loc_id', 'dropoff_loc_id', 'trip_distance', 'fare_amount']

        trips = pd.concat([trips, new_trip]).reset_index(drop=True)
        trips.dropna(how='all', axis=0, inplace=True)

        dupe_check = trips[trips.duplicated(subset=sub_set, keep=False)]
        dupe_check = dupe_check.query(f'trip_id=={trip_id}')

        if len(dupe_check) == 0:
            trips.to_csv(f'{self.data_dir}/trips.csv', index=False)
            return trip_id
        else:
            raise SakayDBError('Trip already exists.')


class SakayDBError(ValueError):
    def __init__(self, exception):
        super().__init__(exception)
