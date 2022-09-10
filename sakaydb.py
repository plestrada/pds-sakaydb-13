"""
This module is created by LT13 for managing ride-hailing data.
LT13: Estrada, Lazaro, Tibayan, Catangui, Lu


Classes
-------

    SakayDB
    SakayDBError
  
  
Main Functions
-------

    add_trip
    add_trips
    delete_trip
    search_trips
    export_data
    generate_statistics
    plot_statistics
    generate_odmatrix

"""

import numpy as np
import pandas as pd
import numpy as np
import os.path
from pathlib import Path
import matplotlib.pyplot as plt


class SakayDB():
    def __init__(self, data_dir):
        """
        This class initializer accepts a string data_dir which is 
        the directory path to where the data files are located.

        Path is stored in the data_dir attribute of the object.

        """
        self.data_dir = data_dir

    def check_create_file(self, filename, columns):
        """
        This function checks whether necessary files for the main
        functions are existing in the provided directory.

        If file does not exist, an empty comma-delimited file with
        the expected columns is created in the directory path.

        Parameters
        ----------
        filename : str
            Directory path
        columns : list
            List of expected columns for when it's needed to create
            an empty file

        Returns
        -------
        None

        """
        if not os.path.exists(filename):
            with open(filename, mode='w', encoding='utf-8') as f:
                f.write(','.join(columns) + '\n')

    def get_driver_id(self, driver):
        """
        This function returns the driver_id of the specified
        trip driver.

        Parameters
        ----------
        driver : str
            Trip driver name in formatted as
            Last name, Given name

        Returns
        -------
        driver_id : int
            Driver id of the trip driver if they are already
            in the drivers database

        None
            If trip driver is not recorded in the drivers database

        """

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
        """
        This function returns the loc_id of the specified
        location. This will be used to get loc_id's of
        specified pickup and dropoff location names.

        Parameters
        ----------
        location : str
            Zone (e.g., Pine View, Legazpi Village)

        Returns
        -------
        loc_id : int
            Location id of the specified pickup or dropoff
            location if it is in the locations database

        None
            If specified location is not in the database

        """
        fn = f'{self.data_dir}/locations.csv'
        df_locations = pd.read_csv(fn)
        cond = (df_locations['loc_name'].str.lower()
                == location.lower().strip())

        if cond.any():
            loc_id = df_locations[cond]['location_id'].item()
            return loc_id
        else:
            return None

    def get_trip_id(self, driver,
                    pickup_datetime,
                    dropoff_datetime,
                    passenger_count,
                    pickup_loc_name,
                    dropoff_loc_name,
                    trip_distance,
                    fare_amount):
        """
        This function returns the trip_id of the specified trip if
        it already exists in the database. This will be used to check
        for duplicate entries to the database.

        Parameters
        ----------
        pickup_datetime : str
            datetime of pickup formatted as "hh:mm:ss,DD-MM-YYYY"

        dropoff_datetime : str
            Datetime of dropoff formatted as "hh:mm:ss,DD-MM-YYYY"

        passenger_count : int
            Number of passengers

        pickup_loc_name : str
            Zone (e.g., Pine View, Legazpi Village)

        dropoff_loc_name : str
            Zone (e.g., Pine View, Legazpi Village)

        trip_distance : float
            Distance in meters

        fare_amount : float
            Fare amount

        Returns
        -------
        trip_id : int
            Trip id of the specified trip if it is already in
            the database.

        None
            If specified trip is not in the database

        """
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
        """
        This function updates the database in case specified
        driver in the main function is not in the database yet.
        This also handles driver id assignment for new drivers.

        Parameters
        ----------
        driver : str
            Trip driver name in formatted as
            Last name, Given name

        Returns
        -------
        None

        """
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

    def add_trip(self, driver,
                 pickup_datetime,
                 dropoff_datetime,
                 passenger_count,
                 pickup_loc_name,
                 dropoff_loc_name,
                 trip_distance,
                 fare_amount):
        """
        This method appends the specified trip data to the end of
        the trips database, if it exists, or creates it, otherwise.


        Parameters
        ----------
        driver : str
            Trip driver name in formatted as
            Last name, Given name

        pickup_datetime : str
            datetime of pickup formatted as
            "hh:mm:ss,DD-MM-YYYY"

        dropoff_datetime : str
            Datetime of dropoff formatted as
            "hh:mm:ss,DD-MM-YYYY"

        passenger_count : int
            Number of passengers

        pickup_loc_name : str
            Zone (e.g., Pine View, Legazpi Village)

        dropoff_loc_name : str
            Zone (e.g., Pine View, Legazpi Village)

        trip_distance : float
            Distance in meters

        fare_amount : float
            Fare amount


        Returns
        -------
        Generated trip id of successfully added trip 


        Raises
        -------
        SakayDBError
            For wrong input formats or when the specified
            trip already exists

        """
        try:
            drv_check = driver.split(',')
            p_datetime = pd.to_datetime(
                pickup_datetime, format='%H:%M:%S,%d-%m-%Y')
            d_datetime = pd.to_datetime(
                dropoff_datetime, format='%H:%M:%S,%d-%m-%Y')
            trip_distance = float(trip_distance)
            fare_amount = float(fare_amount)

        except Exception as e:
            raise SakayDBError(f'Wrong input format: {e}')

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

    def add_trips(self, trips_list):
        """
        This method accepts a list of trips in the
        form of dictionaries. It will add each trip
        to the database and return a list of trip_ids
        of successfully added trips.

        If a trip is already in the database, it will
        skip it and print a warning instead.

        Parameters
        ----------
        trips_list : list
            List of dictionaries with the following keys
            and values

            * driver - str, trip driver name in formatted
                       as Last name, Given name
            * pickup_datetime - str, datetime of pickup
                       formatted as "hh:mm:ss,DD-MM-YYYY"
            * dropoff_datetime - str, datetime of dropoff
                       formatted as "hh:mm:ss,DD-MM-YYYY"
            * passenger_count - int, number of passengers
            * pickup_loc_name - str, zone
                       (e.g., Pine View, Legazpi Village)
            * dropoff_loc_name - str, zone
                       (e.g., Pine View, Legazpi Village)
            * trip_distance - float, distance in meters
            * fare_amount - float, fare amount


        Returns
        -------
        List of trip ids successfully added to the database


        Prints
        -------
        Warning
             If a trip is already in the database or if
             a trip has invalid or incomplete information.
             Note: Both of these cases are skipped.

        """
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
                    print(
                        f'Warning: trip index {i} is already in the database. Skipping...'
                    )

            except:
                print(
                    f'Warning: trip index {i} has invalid or incomplete information. Skipping...'
                )

        return trip_ids

    def delete_trip(self, tr_id):
        """
        This method accepts a trip id to delete then removes
        it from the trips database. 

        Parameters
        ----------
        tr_id : int
            Trip id to delete from the database

        Raises
        -------
        SakayDBError
            If the trip_id is not found

        """
        try:
            fn = f'{self.data_dir}/trips.csv'
            df_trips = pd.read_csv(fn)
        except Exception as e:
            raise SakayDBError(f'{e}')

        tr_id_check = df_trips.loc[df_trips['trip_id'] == tr_id, :]

        if len(tr_id_check) == 0:
            raise SakayDBError(f'trip_id cannot be found')
        else:
            df_trips = df_trips.loc[df_trips['trip_id'] != tr_id, :]
            df_trips.to_csv(fn, encoding='utf-8', index=False)

    def search_input_check(self, k, v):
        """
        This function is used to check the inputs of the
        search_trips function.

        Parameters
        ----------
        k : str
            Variable to check format for;
            Expects either driver_id, passenger_count, pickup
            or dropoff datetime, trip_distance, fare_amount

        v : str, int, or float
            Corresponding value of specified variable for
            format checking

        Raises
        -------
        SakayDBError
            If expected format is not followed

        """
        if k == 'driver_id' or k == 'passenger_count':
            if isinstance(v, int):
                pass
            else:
                raise SakayDBError(f'{k} must be an integer')
        elif k == 'pickup_datetime' or k == 'dropoff_datetime':
            try:
                datetime_check = pd.to_datetime(v, format='%H:%M:%S,%d-%m-%Y')
            except Exception as e:
                raise SakayDBError(f'Wrong input format: {e}')
        elif k == 'trip_distance' or k == 'fare_amount':
            if isinstance(v, int) or isinstance(v, float):
                pass
            else:
                raise SakayDBError(f'{k} must be a number')

    def search_trips(self, **kwargs):
        """
        This method looks for specific trips in the database
        that matches the specified criteria. This accepts
        keyworded arguments.


        Parameters
        ----------
        driver_id : int or tuple
            Driver id

        pickup_datetime : str or tuple
            Datetime of pickup formatted as
            "hh:mm:ss,DD-MM-YYYY"

        dropoff_datetime : str or tuple
            Datetime of dropoff formatted as
            "hh:mm:ss,DD-MM-YYYY"

        passenger_count : int or tuple
            Number of passengers as integer

        trip_distance : float or tuple
            Distance in meters

        fare_amount : float or tuple
            fare amount


        Note: Tuple inputs can follow any of the
        following cases

        * Case 1: tuple like (value,None) - returns all
                  entries from value (end inclusive)
                  
        * Case 2: tuple like (None,value) returns all
                  entries up to value (end inclusive)
                  
        * Case 3: tuple like (value1,value2) returns values
                  between value1 and value2 (end inclusive)


        Returns
        -------
        Dataframe with all the entries aligned with
        search key and values


        Raises
        -------
        SakayDBError
            For invalid keyword arguments, wrong input
            formats, and when no input is provided.

        """
        valid_args = ['driver_id', 'pickup_datetime', 'dropoff_datetime',
                      'passenger_count', 'trip_distance', 'fare_amount']

        if len(kwargs.keys()) == 0:
            raise SakayDBError('No input provided')
        elif any(x not in valid_args for x in kwargs.keys()):
            raise SakayDBError('Invalid argument keyword')
        else:
            fn = f'{self.data_dir}/trips.csv'
            cols = ['trip_id', 'driver_id', 'pickup_datetime',
                    'dropoff_datetime', 'passenger_count', 'pickup_loc_id',
                    'dropoff_loc_id', 'trip_distance', 'fare_amount']

            self.check_create_file(fn, cols)
            df_trips = pd.read_csv(fn)

            if len(df_trips) == 0:
                return []

            df_trips['pickup_datetime'] = pd.to_datetime(
                df_trips['pickup_datetime'], format='%H:%M:%S,%d-%m-%Y')
            df_trips['dropoff_datetime'] = pd.to_datetime(
                df_trips['dropoff_datetime'], format='%H:%M:%S,%d-%m-%Y')

            for k, v in kwargs.items():
                if isinstance(v, tuple):
                    inp_check = len(v)
                    if inp_check > 2:
                        raise SakayDBError('Invalid tuple input')
                    else:
                        if v[1] is None:
                            self.search_input_check(k, v[0])
                            if k == 'pickup_datetime' or k == 'dropoff_datetime':
                                v_0 = pd.to_datetime(
                                    v[0], format='%H:%M:%S,%d-%m-%Y')
                                df_trips = df_trips[df_trips[k] >= v_0]
                            else:
                                df_trips = df_trips[df_trips[k] >= v[0]]
                                df_trips = df_trips.sort_values(
                                    by=k, ascending=True)

                        elif v[0] is None:
                            self.search_input_check(k, v[1])
                            if k == 'pickup_datetime' or k == 'dropoff_datetime':
                                v_1 = pd.to_datetime(
                                    v[1], format='%H:%M:%S,%d-%m-%Y')
                                df_trips = df_trips[df_trips[k] >= v_1]
                            else:
                                df_trips = df_trips[df_trips[k] <= v[1]]
                                df_trips = df_trips.sort_values(
                                    by=k, ascending=True)

                        elif v[0] is not None and v[1] is not None:
                            self.search_input_check(k, v[0])
                            self.search_input_check(k, v[1])
                            if k == 'pickup_datetime' or k == 'dropoff_datetime':
                                v_0 = pd.to_datetime(
                                    v[0], format='%H:%M:%S,%d-%m-%Y')
                                v_1 = pd.to_datetime(
                                    v[1], format='%H:%M:%S,%d-%m-%Y')
                                df_trips = df_trips[(df_trips[k] >= v_0) & (
                                    df_trips[k] <= v_1)]
                            else:
                                df_trips = df_trips[(df_trips[k] >= v[0]) & (
                                    df_trips[k] <= v[1])]
                                df_trips = df_trips.sort_values(
                                    by=k, ascending=True)

                else:
                    self.search_input_check(k, v)
                    if k == 'pickup_datetime' or k == 'dropoff_datetime':
                        v = pd.to_datetime(v, format='%H:%M:%S,%d-%m-%Y')
                    else:
                        df_trips = df_trips[df_trips[k] == v]
                        df_trips = df_trips.sort_values(by=k, ascending=True)

            df_trips['pickup_datetime'] = df_trips['pickup_datetime']\
                .dt.strftime('%H:%M:%S,%d-%m-%Y')
            df_trips['dropoff_datetime'] = df_trips['dropoff_datetime']\
                .dt.strftime('%H:%M:%S,%d-%m-%Y')

            return df_trips

    def export_data(self):
        """
        This method returns a formatted dataframe that
        is ready for export.

        Returns
        -------
        Dataframe with the following columns

            * driver_lastname - str, trip driver last name
            * driver_givenname - str, trip driver last name
            * pickup_datetime - str, datetime of pickup formatted
                                as "hh:mm:ss,DD-MM-YYYY"
            * dropoff_datetime - str, datetime of dropoff formatted
                                as "hh:mm:ss,DD-MM-YYYY"
            * passenger_count - int, number of passengers
            * pickup_loc_name - str, zone
                                (e.g., Pine View, Legazpi Village)
            * dropoff_loc_name - str, zone
                                (e.g., Pine View, Legazpi Village)
            * trip_distance - float, distance in meters
            * fare_amount - float, fare amount

        """
        dr_fn = f'{self.data_dir}/drivers.csv'
        loc_fn = f'{self.data_dir}/locations.csv'
        tr_fn = f'{self.data_dir}/trips.csv'

        dr_cols = ['driver_id', 'given_name', 'last_name']
        loc_cols = ['location_id', 'loc_name']
        tr_cols = ['trip_id', 'driver_id', 'pickup_datetime',
                   'dropoff_datetime', 'passenger_count', 'pickup_loc_id',
                   'dropoff_loc_id', 'trip_distance', 'fare_amount']

        self.check_create_file(dr_fn, dr_cols)
        self.check_create_file(loc_fn, loc_cols)
        self.check_create_file(tr_fn, tr_cols)

        drivers = pd.read_csv(dr_fn)
        locations = pd.read_csv(loc_fn)
        trips = pd.read_csv(tr_fn)

        drivers['last_name'] = drivers['last_name'].str.capitalize()
        drivers['given_name'] = drivers['given_name'].str.capitalize()

        trips = trips.merge(locations, left_on='pickup_loc_id',
                            right_on='location_id', how="left")

        trips.rename(columns={'loc_name': 'pickup_loc_name'}, inplace=True)

        trips = trips.merge(locations, left_on='dropoff_loc_id',
                            right_on='location_id', how="left")

        trips.rename(columns={'loc_name': 'dropoff_loc_name'}, inplace=True)

        merged_df = trips.merge(drivers, left_on='driver_id',
                                right_on='driver_id', how="outer")

        merged_df = merged_df.sort_values(by='trip_id')
        merged_df.rename(columns={'given_name': 'driver_givenname',
                                  'last_name': 'driver_lastname'}, inplace=True)

        merged_df['passenger_count'] = merged_df['passenger_count'].astype(
            'int64')
        merged_df['trip_distance'] = merged_df['trip_distance'].astype(
            'float64')
        merged_df['fare_amount'] = merged_df['fare_amount'].astype('float64')

        merged_df = merged_df.sort_values(by='trip_id',
                                          ascending=True)

        df_export = merged_df[['driver_lastname', 'driver_givenname', 'pickup_datetime',
                               'dropoff_datetime', 'passenger_count', 'pickup_loc_name',
                               'dropoff_loc_name', 'trip_distance', 'fare_amount']].copy()

        return df_export

    def stat_trips(self, df_trips):
        """
        This function will be used in the main
        generate_statistics function. This is used
        to compute stats for trip and returns a
        dictionary where key is day name (e.g., Monday),
        value is the average number of vehicle trips with
        pick-ups for that day name in the entire dataset

        Parameters
        ----------
        df_trips : DataFrame
            Trips database

        Returns
        -------
        Dictionary

        """
        df_trips['pickup_datetime'] = pd.to_datetime(df_trips['pickup_datetime'],
                                                     format='%H:%M:%S,%d-%m-%Y')

        df_trips['pickup_date'] = df_trips['pickup_datetime']\
            .dt.strftime('%Y-%m-%d')

        df_trips['day_of_week'] = df_trips['pickup_datetime'].dt.day_name()
        daily_trips = df_trips.groupby(['pickup_date'], as_index=False)\
            .agg({'trip_id': 'count', 'day_of_week': 'first'})

        per_day_df = daily_trips.groupby('day_of_week', as_index=False).mean()
        per_day_df['day_of_week'] = pd.Categorical(
            values=per_day_df['day_of_week'],
            categories=['Monday', 'Tuesday', 'Wednesday',
                        'Thursday', 'Friday', 'Saturday', 'Sunday'])

        per_day_df.set_index('day_of_week',  inplace=True)
        per_day_df = per_day_df.sort_values(by='day_of_week', ascending=True)
        per_day_dict = per_day_df.to_dict()
        return per_day_dict['trip_id']

    def stat_passenger(self, df_trips):
        """
        This function will be used in the main
        generate_statistics function. This is
        used to compute stats for passenger count
        and returns a dictionary where key is each
        unique passenger_count, value is another
        dictionary with day name (e.g., Monday) as key,
        and value is the average number of vehicle trips
        with pick-ups for that day name in the entire dataset.

        Parameters
        ----------
        df_trips : DataFrame
            Trips database

        Returns
        -------
        Dictionary

        """
        pass_dict = dict()
        for val in df_trips['passenger_count'].unique():
            df_trips_loop = df_trips[df_trips['passenger_count'] == val].copy()
            pass_val = self.stat_trips(df_trips_loop)
            pass_dict[val] = pass_val
        return pass_dict

    def stat_driver(self, df_trips):
        """
        This function will be used in the main
        generate_statistics function. This is
        used to compute stats for drivers and
        returns a dictionary where key is driver
        name following the format Last name, Given name,
        value is another dictionary with day name
        as key and average number of vehicle trips
        of that driver for that day name as value.     

        Parameters
        ----------
        df_trips : DataFrame
            Trips database

        Returns
        -------
        Dictionary

        """
        fn = f'{self.data_dir}/drivers.csv'
        cols = ['driver_id', 'given_name', 'last_name']

        self.check_create_file(fn, cols)
        df_driver = pd.read_csv(fn)

        df_driver['driver_name'] = df_driver['last_name'] + \
            ', ' + df_driver['given_name']
        df_driver = df_driver[['driver_id', 'driver_name']]

        df_trips = df_trips.merge(df_driver, on='driver_id', how='left')
        drv_dict = dict()
        for name in df_trips['driver_name'].unique():
            df_trips_loop = df_trips[df_trips['driver_name'] == name].copy()
            drv_val = self.stat_trips(df_trips_loop)
            drv_dict[name] = drv_val
        return drv_dict

    def generate_statistics(self, stat):
        """
        This method returns a dictionary of
        summary statistics for select variables
        depending on the stat parameter passed to it.

        Parameters
        ----------
        stat : str
            Specifies which summary stats to compute;
            Can either be trip, passenger, driver,
            or all

            * trip - average number of vehicle trips
                     per day of week
            * passenger - average number of vehicle
                     trips per day for each of the
                     different passenger counts
            * driver - average number of vehicle trips
                     per day for each of the drivers
            * all - computes all statistics for trip,
                     passenger, and driver

        Returns
        -------
        Dictionary

        """
        fn = f'{self.data_dir}/trips.csv'
        cols = ['trip_id', 'driver_id', 'pickup_datetime',
                'dropoff_datetime', 'passenger_count', 'pickup_loc_id',
                'dropoff_loc_id', 'trip_distance', 'fare_amount']

        self.check_create_file(fn, cols)
        df_trips = pd.read_csv(fn)

        stats_list = ['trip', 'passenger', 'driver', 'all']

        if stat not in stats_list:
            raise SakayDBError('Unknown stat input')
        else:
            if stat == 'trip':
                return self.stat_trips(df_trips)
            elif stat == 'passenger':
                return self.stat_passenger(df_trips)
            elif stat == 'driver':
                return self.stat_driver(df_trips)
            elif stat == 'all':
                all_dict = dict()
                all_dict['trip'] = self.stat_trips(df_trips)
                all_dict['passenger'] = self.stat_passenger(df_trips)
                all_dict['driver'] = self.stat_driver(df_trips)
                return all_dict

    def plot_statistics(self, stat):
        """
        This method plots summary statistics for
        select variables depending on the stat
        parameter passed to it.

        Parameters
        ----------
        stat : str
            Specifies which summary stats to plot;
            Can either be trip, passenger, driver

            * trip - creates a bar plot of the average 
                     number of vehicle trips per day of week
            * passenger - creates a line plot showing the
                     average number of vehicle trips per day
                     for each of the different passenger counts
            * driver - creates a 7x1 grid that plots the top 5
                     drivers with the top average trips per
                     day as a horizontal bar plot

        Returns
        -------
        Figure or axes

        """
        fn = f'{self.data_dir}/trips.csv'
        cols = ['trip_id', 'driver_id', 'pickup_datetime',
                'dropoff_datetime', 'passenger_count', 'pickup_loc_id',
                'dropoff_loc_id', 'trip_distance', 'fare_amount']

        self.check_create_file(fn, cols)
        df_trips = pd.read_csv(fn)

        stats_list = ['trip', 'passenger', 'driver']

        if stat not in stats_list:
            raise SakayDBError('Unknown stat input')
        else:
            if stat == 'trip':
                fig, ax = plt.subplots(figsize=(12, 8))
                trips = self.generate_statistics('trip')
                trips = pd.DataFrame(trips, index=['Ave Trips']).T
                trips.plot(kind='bar', ax=ax, legend=False)

                plt.title('Average trips per day')
                plt.xlabel('Day of week')
                plt.ylabel('Ave Trips')
                plt.xticks(rotation=0)
                plt.show()
                return ax

            elif stat == 'passenger':
                fig, ax = plt.subplots(figsize=(12, 8))
                psgr = self.generate_statistics('passenger')
                psgr = pd.DataFrame(psgr)[range(4)]
                psgr.plot(marker='o', ax=ax, legend=False)

                plt.xlabel('Day of week')
                plt.ylabel('Ave Trips')
                plt.xticks(rotation=0)
                plt.legend()
                plt.show()
                return ax

            elif stat == 'driver':
                fig, ax = plt.subplots(nrows=7, ncols=1, figsize=(8, 25))
                driver = self.generate_statistics('driver')
                driver = pd.DataFrame(driver).T

                for i, day in enumerate(driver.columns):
                    driver_day = driver[day].sort_index(
                        ascending=False).nlargest(5).sort_values(ascending=True)
                    driver_day.plot(kind='barh', ax=ax[i], sharex=True)
                    ax[i].legend(loc='lower right')
                    ax[i].set_xlim(0, 3)

                plt.xlabel('Ave Trips')
                plt.show()
                return fig

    def generate_odmatrix(self, date_range=None):
        """
        This method creates a dataframe that maps
        the average daily number of vehicle trips
        that occured for each origin-destination
        combination within the date_range specified.

        Parameters
        ----------
        date_range : Tuple or None
            A range search that takes a tuple of
            datetime strings, and filters trips
            based on pickup_datetime. Defaults to None,
            in which case all dates are included

        Note: Tuple inputs can follow any of the
        following cases

        * Case 1: tuple like (value,None) returns all entries
                  from value (end inclusive)
                  
        * Case 2: tuple like (None,value) returns all entries
                  up to value (end inclusive)
                  
        * Case 3: tuple like (value1,value2) returns values
                  between value1 and value2 (end inclusive)


        Returns
        -------
        DataFrame


        Raises
        -------
        SakayDBError
            If date_range input is invalid

        """
        loc_fn = f'{self.data_dir}/locations.csv'
        tr_fn = f'{self.data_dir}/trips.csv'

        loc_cols = ['location_id', 'loc_name']
        tr_cols = ['trip_id', 'driver_id', 'pickup_datetime',
                   'dropoff_datetime', 'passenger_count', 'pickup_loc_id',
                   'dropoff_loc_id', 'trip_distance', 'fare_amount']

        self.check_create_file(loc_fn, loc_cols)
        self.check_create_file(tr_fn, tr_cols)

        locations = pd.read_csv(loc_fn)
        trips = pd.read_csv(tr_fn)

        trips['pickup_datetime'] = pd.to_datetime(
            trips['pickup_datetime'], format='%H:%M:%S,%d-%m-%Y')
        trips['count'] = 1

        try:
            if len(date_range) > 2:
                raise SakayDBError('Invalid date_range input')

        except Exception:
            pass

        if date_range is None:
            trips = trips.copy()

        elif date_range[1] is None:
            try:
                p_datetime = pd.to_datetime(
                    date_range[0], format='%H:%M:%S,%d-%m-%Y')
            except:
                raise SakayDBError('Invalid date_range input')

            date_range = [pd.to_datetime(
                x, format='%H:%M:%S,%d-%m-%Y') for x in date_range]
            trips = trips[trips['pickup_datetime'] >= date_range[0]]

        elif date_range[0] is None:
            try:
                p_datetime = pd.to_datetime(
                    date_range[1], format='%H:%M:%S,%d-%m-%Y')
            except:
                raise SakayDBError('Invalid date_range input')

            date_range = [pd.to_datetime(
                x, format='%H:%M:%S,%d-%m-%Y') for x in date_range]
            trips = trips[trips['pickup_datetime'] <= date_range[1]]

        elif date_range[0] is not None and date_range[1] is not None:
            try:
                p1_datetime = pd.to_datetime(
                    date_range[0], format='%H:%M:%S,%d-%m-%Y')
                p2_datetime = pd.to_datetime(
                    date_range[1], format='%H:%M:%S,%d-%m-%Y')
            except:
                raise SakayDBError('Invalid date_range input')

            date_range = [pd.to_datetime(
                x, format='%H:%M:%S,%d-%m-%Y') for x in date_range]
            trips = trips[(trips['pickup_datetime'] > date_range[0]) &
                          (trips['pickup_datetime'] <= date_range[1])]

        trips = trips.merge(locations, left_on='pickup_loc_id',
                            right_on='location_id', how='left')\
            .rename(columns={'loc_name': 'pickup_loc_name'})

        trips = trips.merge(locations, left_on='dropoff_loc_id',
                            right_on='location_id', how='left')\
            .rename(columns={'loc_name': 'dropoff_loc_name'})

        trips = trips.drop(columns=['location_id_x', 'location_id_y',
                                    'pickup_loc_id', 'dropoff_loc_id'])

        trips = trips[['count', 'pickup_loc_name',
                       'dropoff_loc_name', 'pickup_datetime']].copy()

        trips['pickup_date'] = trips['pickup_datetime'].dt.strftime('%Y-%m-%d')
        trips.drop(columns='pickup_datetime', inplace=True)

        od_matrix = trips.groupby(['pickup_loc_name', 'dropoff_loc_name', 'pickup_date'],
                                  as_index=False).sum()

        od_matrix = od_matrix.pivot_table(columns='pickup_loc_name',
                                          index='dropoff_loc_name', aggfunc='mean', fill_value=0)

        od_matrix.columns = [x[1] for x in od_matrix.columns]
        return od_matrix


class SakayDBError(ValueError):
    def __init__(self, exception):
        super().__init__(exception)
