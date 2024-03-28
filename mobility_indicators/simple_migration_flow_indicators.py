import pandas as pd
import datetime as dt
from helper_functions.finegrained_helper import read_fine_grained,customer_signals_analysis,calculate_date_values,filter_customers,process_fine_grained
import skmob
from skmob.measures.individual import home_location
from helper_functions.tower_helper import read_tower_data
from helper_functions.utilities import get_location_level

def frequency_based_aggregation(data, spatial_level, period='month', time_format='%Y%m%d'):
    df = data.copy()
    if type(df['time'].iloc[0]) != pd._libs.tslibs.timestamps.Timestamp:
        df['time'] = pd.to_datetime(df['time'], format=time_format)

    loc_level = get_location_level(spatial_level)
    if loc_level is None:
        raise ValueError("Invalid value for spatial_level. It should be 'site', 'district', or 'city'.")

    df['year'] = df['time'].dt.year
    if 'day' in period:
        df['date'] = df['time'].dt.date
    elif 'week' in period:
        df['week'] = df['time'].dt.isocalendar().week
    elif 'month' in period:
        df['month'] = df['time'].dt.month
    else:
        raise ValueError("Invalid value for period. It should be 'day', 'week' or 'month'.")

    if 'day' in period:
        grouped = df.groupby(['customer_id', 'year', 'date'])
        period_unit = 'date'
    elif 'week' in period:
        grouped = df.groupby(['customer_id', 'year', 'week'])
        period_unit = 'week'
    else:
        grouped = df.groupby(['customer_id', 'year', 'month'])
        period_unit = 'month'

    most_frequent_location = grouped[loc_level].agg(lambda x: x.mode()[0] if not x.mode().empty else pd.NA)

    aggregated_data = pd.DataFrame({
        loc_level: most_frequent_location,
    }).reset_index()

    unique_users = df['customer_id'].unique()
    unique_years = df['year'].unique()
    if 'day' in period:
        unique_periods = df['date'].unique()
    elif 'week' in period:
        unique_periods = df['week'].unique()  # ISO weeks
    else:
        unique_periods = df['month'].unique()  # Months

    all_combinations = pd.MultiIndex.from_product([unique_users, unique_years, unique_periods],
                                                  names=['customer_id', 'year', period_unit]).to_frame(index=False)

    merged_data = pd.merge(all_combinations, aggregated_data, on=['customer_id', 'year', period_unit], how='left').reset_index(drop=True)

    return merged_data.sort_values(by=['customer_id', 'year', period_unit])




def sort_approach(df, *args):

    if 'day' not in df.columns:
        df['day'] = df['time'].dt.day
    if 'month' not in df.columns:
        df['month'] = df['time'].dt.month
    if 'hour' not in df.columns:
        df['hour'] = df['time'].dt.hour
    #df = df[df['hour'] != 23]
    df['count'] = 1
    # Using *args to dynamically create the pivot table index
    df_pivot = pd.pivot_table(df[(df['hour'] > 21) | (df['hour'] < 8)],
                              index=['day', 'month', *args], values=['count'],
                              aggfunc=sum).reset_index()

    sort_order = [args[0], 'day', 'month', 'count'] if args else ['day', 'month', 'count']
    df_pivot = df_pivot.sort_values(by=sort_order, ascending=True).reset_index(drop=True)
    return df_pivot


def scikit_approach(df, suffix, *args):

    if 'day' not in df.columns:
        df['day'] = df['time'].dt.day
    if 'month' not in df.columns:
        df['month'] = df['time'].dt.month
    if 'hour' not in df.columns:
        df['hour'] = df['time'].dt.hour

    tdf2 = skmob.TrajDataFrame(df, latitude=f'lat_{suffix}', longitude=f'lng_{suffix}', datetime='time', user_id='customer_id').sort_values(
        by=['uid', 'datetime'])
    tdf2 = tdf2[tdf2.lat.isnull() == False]
    tdf2 = tdf2.sort_values(by=['uid', 'datetime'], ascending=False)
    tdf2 = tdf2[['lat', 'lng', 'datetime', 'uid', 'day']].reset_index(drop=True)
    Day = []
    print("Daily home locations are being calculated...")

    for j in tdf2.day.unique():
        hl_df_daily = home_location(tdf2[tdf2['day'] == j], start_night='22:00', end_night='7:00', show_progress=True)
        hl_df_daily['day'] = j
        Day.append(hl_df_daily)
        print("Day" + str(j) + " is completed.")
    print("The calculation of daily locations is done.")

    df_daily = pd.concat(Day).reset_index()
    df_daily = df_daily.rename({'uid': 'customer_id'}, axis=1)
    df_daily = df_daily.drop_duplicates(['customer_id', 'day'], keep='last').reset_index(drop=True)
    df = df[['day', 'month','customer_id', *args]]
    df_daily = df_daily.merge(df, on=['customer_id','day'], how='left')
    df_daily = df_daily.drop_duplicates(['customer_id', 'day'], keep='last').reset_index(drop=True)

    return df_daily
def daily_home_location_series(file_path, data_direction, spatial_level,
                                             approach='sort_approach', cust_list=None, file_list =None,tower_location=None,df_tower=None):

    if not isinstance(file_list, list):
        raise ValueError("L must be a list.")

    if file_list is None or len(file_list) == 0:
        file_list = ['7', '7_2', '7_3', '7_4', '7_5', '7_6']  # Default list if not provided or empty

    accumulated_df = pd.DataFrame()
    if data_direction == 'OUTGOING':
        suffix = 'caller'
    elif data_direction == 'INCOMING':
        suffix = 'callee'
    else:
        raise ValueError("Invalid value for data_direction. It should be 'OUTGOING' or 'INCOMING'.")

    for i in file_list:

        file_path_formatted = file_path.format(i)
        df=process_fine_grained(file_path_formatted, data_direction, tower_location, cust_list, df_tower)

        if spatial_level == 'site':
            column_name = f'site_id_{suffix}'
        elif spatial_level == 'district':
            column_name = f'city_district_id_{suffix}'
        else:
            raise ValueError("Invalid value for spatial_level. It should be 'site' or 'district'.")
        # Choose the approach based on user input
        if approach == 'sort_approach':
            r = sort_approach(df, 'customer_id', column_name, f'segment_{suffix}')
        elif approach == 'scikit_approach':
            r = scikit_approach(df, suffix,  column_name, f'segment_{suffix}')
        else:
            raise ValueError("Invalid value for approach. It should be 'sort_approach' or 'scikit_approach'.")

        accumulated_df = pd.concat([accumulated_df, r], ignore_index=True)
        print(i)

    accumulated_df['year'] = 2023
    accumulated_df['date'] = pd.to_datetime(accumulated_df[['year', 'month', 'day']])

    if spatial_level == 'site':
        accumulated_df = accumulated_df[['date', 'customer_id', f'site_id_{suffix}', f'segment_{suffix}']]
    elif spatial_level == 'district':
        accumulated_df = accumulated_df[['date', 'customer_id', f'city_district_id_{suffix}', f'segment_{suffix}']]
    else:
        raise ValueError("Invalid value for spatial_level. It should be 'site' or 'district'.")

    accumulated_df['date'] = pd.to_datetime(accumulated_df['date'])
    accumulated_df['date'] = accumulated_df['date'].dt.strftime('%Y%m%d')

    return accumulated_df