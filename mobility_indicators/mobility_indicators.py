from collections import Counter

import numpy as np
import pandas as pd
import skmob
from helper_functions.tower_helper import *
from helper_functions.utilities import get_location_level
from scipy.stats import entropy
from skmob.measures.individual import distance_straight_line
from skmob.measures.individual import home_location
from skmob.measures.individual import max_distance_from_home
from skmob.measures.individual import maximum_distance
from skmob.measures.individual import number_of_locations
from skmob.measures.individual import number_of_visits
from skmob.measures.individual import radius_of_gyration
from skmob.measures.individual import random_entropy
from skmob.measures.individual import uncorrelated_entropy
from skmob.measures.individual import waiting_times
from datetime import timedelta
from helper_functions.utilities import *

def detect_trips(df, spatial_level, temporal_granularity='day', time_format='%Y%m%d'):
    validate_dataframe_columns(df, data_type='individual')
    if type(df['time'].iloc[0]) != pd._libs.tslibs.timestamps.Timestamp:
        df['time'] = pd.to_datetime(df['time'], format=time_format)
    else:
        pass
    df = df.sort_values(by=['customer_id', 'time'])
    loc_level = get_location_level(spatial_level)
    if loc_level is None:
        raise ValueError("Invalid value for spatial_level. It should be 'site', 'district', or 'city'.")

    if temporal_granularity == 'month':
        df = frequency_based_aggregation(df, spatial_level, temporal_granularity, time_format)
        time_delta = pd.DateOffset(months=1)
    elif temporal_granularity == 'week':
        df = frequency_based_aggregation(df, spatial_level, temporal_granularity, time_format)
        time_delta = pd.DateOffset(weeks=1)
    elif temporal_granularity == 'day':
        time_delta = pd.DateOffset(days=1)
    elif temporal_granularity == 'hour':
        time_delta = pd.DateOffset(hours=1)
    else:
        raise ValueError("Invalid value for temporal_granularity. It should be 'month', 'week', 'day', or 'hour'.")

    trips = []
    last_location = None
    trip_start = None
    current_user = None
    trip_id = 1

    for index, row in df.iterrows():
        if current_user != row['customer_id']:
            if current_user is not None and last_location is not None:
                trips.append(
                    [trip_id, current_user, trip_start, row['time'] - time_delta, last_location, last_location])
                trip_id += 1

            current_user = row['customer_id']
            last_location = row[loc_level]
            trip_start = row['time']

        elif last_location != row[loc_level]:
            trips.append([trip_id, current_user, trip_start, row['time'] - time_delta, last_location, row[loc_level]])
            trip_id += 1
            last_location = row[loc_level]
            trip_start = row['time']

    trips_df = pd.DataFrame(trips, columns=['trip_id', 'customer_id', 'trip_start_date', 'trip_end_date', 'origin_loc',
                                            'destination_loc'])

    return trips_df


def o_d_matrix(data, spatial_aggregation_level, temporal_aggregation_level, spatial_granularity_level,
               temporal_granularity_level, time_format='%Y%m%d'):
    validate_dataframe_columns(data, data_type='individual')
    df = data.copy()

    loc_level = get_location_level(spatial_aggregation_level)
    if loc_level is None:
        raise ValueError("Invalid value for spatial_level. It should be 'site', 'district', or 'city'.")

    if 'week' in temporal_aggregation_level:
        df['week'] = df['time'].dt.isocalendar().week
    elif 'month' in temporal_aggregation_level:
        df['month'] = df['time'].dt.month
    else:
        raise ValueError("Invalid value for period. It should be 'week' or 'month'.")

    trips_df = detect_trips(df[['customer_id', 'time', loc_level]], spatial_granularity_level,
                            temporal_granularity_level, time_format)
    trips_df = trips_df.merge(df[['customer_id', 'segment']].drop_duplicates(subset=['customer_id']), on='customer_id',
                              how='left')
    trips_df['trip_start_date'] = pd.to_datetime(trips_df['trip_start_date'])
    trips_df['trip_end_date'] = pd.to_datetime(trips_df['trip_end_date'])
    trips_df = trips_df[trips_df['origin_loc'] != trips_df['destination_loc']]
    adjusted_trips_df = trips_df.apply(lambda row: adjust_trips_to_period(row, temporal_aggregation_level), axis=1)

    adjusted_trips_df['week'] = adjusted_trips_df['trip_start_date'].dt.strftime('%Y-%U')

    aggregated_trips = adjusted_trips_df.groupby(
        [temporal_aggregation_level, 'segment', 'origin_loc', 'destination_loc']).size().reset_index(name='flow_count')

    return aggregated_trips


def adjust_trips_to_period(row, period='week'):

    if period == 'week':
        midpoint = row['trip_start_date'] + (row['trip_end_date'] - row['trip_start_date']) / 2
        start_of_week = midpoint - timedelta(days=midpoint.weekday())
        end_of_week = start_of_week + timedelta(days=6)
        row['trip_start_date'] = start_of_week
        row['trip_end_date'] = end_of_week

    elif period == 'month':
        start_of_month = row['trip_start_date'].replace(day=1)
        end_of_month = (start_of_month + pd.offsets.MonthEnd(1)).date()
        row['trip_start_date'] = start_of_month
        row['trip_end_date'] = end_of_month
    else:
        raise ValueError("Invalid period specified. Choose 'week', or 'month'.")

    return row


def calculate_scikit_indicators(df,dict_site_to_lat,dict_site_to_long):

    """
    Compute a set of scikit-mobility indicators give the dataframe.
    
    Parameters
    -----------
    df : pandas DataFrame
        the dataframe of the individual.
    df: assumed to have; customer_id, location, time 
        
    Returns
    -------
    pandas DataFrame
        the individual mobility indicators are calculated for the group of individuals.
    """
    validate_dataframe_columns(df, data_type='individual')
    print("Scikit_indicators_are_being_calculated")
    df['lat'] = df['site_id'].map(dict_site_to_lat)
    df['long'] = df['site_id'].map(dict_site_to_long)

    tdf = skmob.TrajDataFrame(df, latitude='lat', longitude='long', datetime = 'time', user_id='customer_id').sort_values(by=['datetime'])
    rg_df_all = radius_of_gyration(tdf)
    #krg_df_all = k_radius_of_gyration(tdf)
    re_df_all = random_entropy(tdf) 
    ure_df_all = uncorrelated_entropy(tdf)
    md_df_all = maximum_distance(tdf)
    dsl_df_all = distance_straight_line(tdf)
    wt_df_all = waiting_times(tdf)
    nol_df_all = number_of_locations(tdf)
    hl_df_all = home_location(tdf)
    mdfh_df_all = max_distance_from_home(tdf)
    nov_df_all = number_of_visits(tdf)
    #lf_df_all = location_frequency(tdf)


    L_all = [re_df_all, ure_df_all, rg_df_all, md_df_all, dsl_df_all, wt_df_all, nol_df_all, hl_df_all, mdfh_df_all, nov_df_all]

    for i in L_all:
        rg_df_all = rg_df_all.merge(i, on = 'uid', how = 'left')
    rg_df_all=rg_df_all.rename({'uid':'customer_id'},axis=1)

    rg_df_all = rg_df_all.drop_duplicates(subset='customer_id')

    return rg_df_all

######### BANDICOOT

def percent_nocturnal(df, night_start=19, night_end=7):

    if len(df) == 0:
        return 0
    else:
        night_filter = df[(df['hour'] > night_start) | (df['hour'] < night_end)].reset_index()

    return len(night_filter) / len(df)

def summary_stats(df):
    return {
        'max': df['interevent_time'].max(),
        'min': df['interevent_time'].min(),
        'mean': df['interevent_time'].mean(),
        'sum': df['interevent_time'].sum()
    }


def calculate_unique_city_counts(df):
    df_processed = df.copy()
    validate_dataframe_columns(df_processed, data_type='individual')
    df_processed['customer_city'] = df_processed.apply(
        lambda x: x['city_id_callee'] if x['call_type'] == 1 else x['city_id_caller'], axis=1)
    df_processed['other_party_city'] = df_processed.apply(
        lambda x: x['city_id_caller'] if x['call_type'] == 1 else x['city_id_callee'], axis=1)
    df_processed = df_processed.dropna(subset=['customer_city', 'other_party_city'], how='all')
    customer_city_count = df_processed.groupby('customer_id')['customer_city'].nunique()
    other_party_city_count = df_processed.groupby('customer_id')['other_party_city'].nunique()
    city_counts = pd.DataFrame({
        'customer_id': customer_city_count.index,
        'cities_been_to_count': customer_city_count.values,
        'cities_in_communication_count': other_party_city_count.values
    }).reset_index(drop=True)

    return city_counts


def calculate_customer_nocturnal(df):
    return df.groupby('customer_id').apply(percent_nocturnal).reset_index().rename(columns={0: 'percentage_nocturnal'})

def entropy_of_antennas(df, normalize=False):
    df_grouped = df.groupby('customer_id')['site_id'].apply(list)
    counter = df_grouped.apply(Counter)
    raw_entropy = counter.apply(lambda x: entropy(list(x.values())))
    n = counter.apply(len)
    if normalize:
        return (raw_entropy / np.log(n)).reset_index().rename(columns={'site_id': 'entropy_of_antennas'})
    else:
        return raw_entropy.reset_index().rename(columns={'site_id': 'entropy_of_antennas'})

def number_of_antennas(df):
    num_antennas = df.groupby('customer_id')['site_id'].nunique()
    return num_antennas.reset_index().rename(columns={'site_id': 'number_of_antennas'})

def frequent_antennas(df, percentage=0.8):
    validate_dataframe_columns(df, data_type='individual')
    df_grouped = df.groupby('customer_id')['site_id'].apply(list)
    counter = df_grouped.apply(Counter)
    frequent_antennas = counter.apply(lambda x: sum(np.array(list(x.values())) >= percentage * sum(list(x.values()))))
    return frequent_antennas.reset_index().rename(columns={'site_id': 'frequent_antennas'})

def calculate_bandicoot_indicators(df):
    validate_dataframe_columns(df, data_type='individual')
    print("Bandicoot_indicators_are_being_calculated...")
    df_nocturnal = calculate_customer_nocturnal(df)
    #df_interevent = calculate_interevent_time(df)
    df_entropy = entropy_of_antennas(df)
    df_number = number_of_antennas(df)
    df_frequent = frequent_antennas(df)

    L = [df_entropy, df_number, df_frequent]# include: df_interevent

    for i in L:
        df_nocturnal = df_nocturnal.merge(i, on = 'customer_id', how = 'left')

    return df_nocturnal


