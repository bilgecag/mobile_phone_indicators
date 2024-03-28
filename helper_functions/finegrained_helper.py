import pandas as pd
import numpy as np
from helper_functions.tower_helper import *
from helper_functions.utilities import *

## Read

def read_fine_grained(dir_finegrained, datatype):
    if datatype=='XDR':
        df = pd.read_csv(dir_finegrained,
                    sep="|", skiprows=2,
                    header=None, encoding='ISO-8859-1')
        df = df.drop([0, 5], axis=1)
        df.columns = ['time', 'customer_id', 'segment', 'site_id']
        df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%d %H')
    elif datatype == 'CDR':
        df = pd.read_csv(dir_finegrained,index_col=0)
        df = df.rename(columns={"CUSTOMER_ID": "customer_id", "DAY": "date", "HOUR": "hour",
                                "CALLE_SITE_ID": "site_id_callee", \
                                "CALER_SITE_ID": "site_id_caller", "CALLEE_SEGMENT": "segment_callee",\
                                "CALER_SEGMENT": "segment_caller",\
                                "CALEE_SEGMENT":"segment_callee",\
                                "CALLER_SEGMENT": "segment_caller"})
        #df['time'] = pd.to_datetime(df['date'], infer_datetime_format=True) + pd.to_timedelta(df['hour'], unit='h')
        df['time'] = pd.to_datetime(df['date'], format="%d/%m/%Y") + pd.to_timedelta(df['hour'], unit='h')
    return df

# Ostype_add function is to deal with changing name of the directory between linux and mac

## Read

def read_cdr_data_tt(file_directory_cdr, file_directory_towers):
    cdr = pd.read_csv(file_directory_cdr, sep=',', encoding="ISO-8859-1", error_bad_lines=False)
    tt = pd.read_csv(file_directory_towers, sep=",", header=0)
    tt = tt[['BTS_ID', 'LAT', 'LONG', 'MX_SAHAIL', 'MX_SAHAIL']]
    tt = tt.rename({'BTS_ID': 'site_id', 'MX_SAHAIL': 'city', 'MX_SILCE': 'district'}, axis=1)
    # cdr = pd.read_csv(tt_cdr, sep=',', encoding="ISO-8859-1", error_bad_lines=False)
    cdr = cdr.rename({'OUTGOING_SITE_ID': 'site_id', 'TIMESTAMP': 'time', 'NUMBER_OF_CALLS': 'total_local',
                      'NUMBER_OF_REFUGEE_CALLS': 'total_refugee', }, axis=1)
    cdr = cdr.merge(tt, on=['site_id'], how='left')
    return cdr


## Aggregation

def fine_grained_to_antenna(df,datatype):
    if datatype == 'XDR':
        segs = ['segment_1', 'segment_2', 'segment_3', 'segment_4',
            'segment_5', 'segment_6', 'segment_7', 'segment_8', 'segment_9',
            'segment_10', 'segment_11', 'segment_12', 'segment_13', 'segment_14']

        dum = pd.get_dummies(df['segment'], prefix='segment')
        df = df.drop_duplicates(subset=['customer_id', 'time', 'site_id'], keep='last')
        df = pd.concat([df, dum], axis=1)
        df = pd.pivot_table(df, index=['site_id', 'time'], values=segs, aggfunc=np.sum).reset_index()
    if datatype == 'CDR':
        df['segment_caller'].fillna(0, inplace=True)
        df['segment_callee'].fillna(0, inplace=True)

        df['segment_caller'] = df['segment_caller'].astype(int)
        df['segment_callee'] = df['segment_callee'].astype(int)

        df['segment_caller_callee'] = df['segment_caller'].astype(str) + "-" + df['segment_callee'].astype(str)

        df=df.groupby(['time', 'site_id_caller', 'site_id_callee', 'segment_caller_callee']).size().reset_index(
            name='call_count')

    return df

## Signal variables

def customer_signals_analysis(df,site_id):

    df['site_count'] = df.groupby('customer_id')[site_id].transform('nunique')
    df['day_count'] = df.groupby('customer_id')['day'].transform('nunique')

    customers_analysis = df.groupby('customer_id').agg({
        'time': 'count',
        #'23_dummy': 'sum',
        #'is_weekend': 'sum',
        'night_dummy': 'sum',
        'day_count': 'first',
        'site_count': 'first'
    }).rename(columns={
        'time': 'signal_count',
        #'23_dummy': 'signal_at_23',
        #'is_weekend': 'signal_on_weekend',
        'night_dummy': 'signal_at_night',
        'day_count': 'unique_days_count',
        'site_count': 'unique_sites_count'
    })

    return customers_analysis.reset_index()

## Filter

def filter_customers(cust_df, signal_count, unique_days_threshold, night_signal_threshold, customer_list=None, include_customers=True):
    """
    Filters customers based on signal count, unique days threshold, and night signal threshold.
    Optionally filters by a list of customer IDs to include or exclude.

    Parameters:
    - cust_df: DataFrame containing customer data.
    - signal_count: Minimum number of signals to include a customer.
    - unique_days_threshold: Minimum number of unique days with signals to include a customer.
    - night_signal_threshold: Minimum number of signals at night to include a customer.
    - customer_list: Optional list of customer IDs to further filter the DataFrame. Default is None.
    - include_customers: Determines whether to include (True) or exclude (False) the customers in the customer_list. Default is True.

    Returns:
    - DataFrame of filtered customers.
    """
    filtered_df = cust_df[
        (cust_df['signal_count'] >= signal_count) &
        (cust_df['unique_days_count'] >= unique_days_threshold) &
        (cust_df['signal_at_night'] > night_signal_threshold)
    ]

    if customer_list is not None:
        if include_customers:
            filtered_df = filtered_df[filtered_df['customer_id'].isin(customer_list)]
        else:
            filtered_df = filtered_df[~filtered_df['customer_id'].isin(customer_list)]

    return filtered_df

def process_fine_grained(file_path_formatted, data_direction, tower_location=None, cust_list=None, df_tower=None):
    suffix, opposite_suffix = get_suffixes_based_on_direction(data_direction)
    df = read_fine_grained(file_path_formatted, datatype="CDR")
    df = calculate_date_values(df)
    cust_df = customer_signals_analysis(df, f'site_id_{suffix}')
    filtered_cust = filter_customers(cust_df, 10, 5, 5, customer_list=cust_list, include_customers=True)[
            'customer_id'].unique().tolist()
    df = df[df['customer_id'].isin(filtered_cust) == True].reset_index(drop=True)

    if df_tower is None:
        if tower_location is None:
            raise ValueError("Either tower_location or df_tower must be provided.")
        df_tower = read_tower_data(tower_location, crs="EPSG:4326")
    df_tower = df_tower[['city_district_id', 'site_id','lat','lng','city_id']]

    df = df.merge(df_tower, right_on='site_id', left_on=f'site_id_{suffix}',how='left'). \
            rename(columns={'city_district_id': f'city_district_id_{suffix}', \
                            'city_id': f'city_id_{suffix}', \
                            'lat': f'lat_{suffix}', \
                            'lng': f'lng_{suffix}'}).drop(columns=['site_id'])

    df = df.merge(df_tower, right_on='site_id', left_on=f'site_id_{opposite_suffix}',how='left'). \
        rename(columns={'city_district_id': f'city_district_id_{opposite_suffix}', \
                        'city_id': f'city_id_{opposite_suffix}', \
                        'lat': f'lat_{opposite_suffix}', \
                        'lng': f'lng_{opposite_suffix}'}).drop(columns=['site_id'])

    df=df[['time', 'customer_id', f'site_id_{suffix}', f'segment_{suffix}',\
            f'site_id_{opposite_suffix}', f'segment_{opposite_suffix}', \
           f'city_id_{opposite_suffix}',f'city_id_{suffix}',\
            f'city_district_id_{opposite_suffix}', f'lat_{opposite_suffix}', f'lng_{opposite_suffix}', \
           f'city_district_id_{suffix}',f'lat_{suffix}',f'lng_{suffix}']]
    return df
## Date variables

def calculate_date_values(df):
    df['time'] = pd.to_datetime(df['time'])
    df['hour'] = df['time'].dt.hour
    df['day'] = df['time'].dt.day
    df['week'] = df['time'].dt.isocalendar().week
    df['month'] = df['time'].dt.month
    df['year'] = df['time'].dt.year
    df['dayofweek'] = df['time'].dt.dayofweek
    #df['is_weekend'] = df['dayofweek'].apply(lambda x: 1 if x >= 5 else 0)
    df['night_dummy'] = df['hour'].apply(lambda x: 1 if x >= 19 or x < 7 else 0)
    #df['23_dummy'] = df['hour'].apply(lambda x: 1 if x == 23 else 0)
    return df

### Filter
def filter_fgrained_by_vars(df, hour, customer_list, city_list, exclude_hour=False, include_city_list=False):
    """
    This function filters the input dataframe by customer_id, city, and hour.
    
    Parameters
    ----------
    df : pandas DataFrame
        The DataFrame to be filtered. This DataFrame is assumed to contain columns named 'customer_id', 'location', and 'time'.
    hour : int
        The hour to filter by. Data for other hours will be excluded.
    customer_list : list
        The list of customers to include in the filtered DataFrame.
    city_list : list
        The list of cities to include or exclude in the filtered DataFrame.
    exclude_hour : bool, optional
        If True, excludes the provided hour instead of including it. Default is False (i.e., include the hour).
    include_city_list : bool, optional
        If True, includes the cities listed in city_list, otherwise, it excludes them. Default is False (i.e., exclude the cities).
    
    Returns
    -------
    pandas DataFrame
        The filtered DataFrame, containing only rows with customer_id, city, and hour as specified.
    """
    site_to_city, site_to_district, site_to_long, site_to_lat = tower_data_to_dict(tower_location=None)
    
    if hour:
        if exclude_hour:
            df=df[df['hour'] != hour].reset_index(drop=True)
        else:
            df=df[df['hour'] == hour].reset_index(drop=True)
    else:
        df = df=df[df['hour'] != 23].reset_index(drop=True)
        
    if city_list:
        df = df[df['site_id'].map(site_to_city).isin(city_list) == include_city_list].reset_index(drop=True)
    
    if customer_list:
        df = df[df['customer_id'].isin(customer_list)].reset_index(drop=True)

    return df


    return df

###Aggregation

