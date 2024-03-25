
import pandas as pd
from helper_functions.tower_helper import read_tower_data
import matplotlib.pyplot as plt
from helper_functions.utilities import *

def read_antenna_data(antenna_location, data_type):
    try:
        if data_type == "XDR":
            df = pd.read_csv(
                antenna_location,
                sep="|",
                header=0, encoding='ISO-8859-1')
            df = df.drop(['Unnamed: 0', 'Unnamed: 4'], axis=1)
            df = df.iloc[1:, :]
            df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
            df = df.rename(columns=lambda x: x.strip())
            df = df.rename(columns={'matcher': 'site_id'})
            df['site_id'] = df['site_id'].astype(int)
            # print('There are {} cell towers in the dataset'.format(df.site_id.nunique()))

        elif data_type == "CDR":
            # Tower data read function for CDR data
            df = pd.read_csv(antenna_location)
            df=df.rename(columns={"CALLER_CALLE_SEGMENT":"segment_caller_callee", "DAY":"date", "HOUR":"hour", "CALLEE_MATCHER":"site_id_callee",\
                                  "CALER_MATCHER":"site_id_caller", "SUM(CALL_DURATION)":"call_duration", "CALLER_CALLE_SEGMENT_T":"call_count"})
            df['time'] = pd.to_datetime(df['date'], format="%d/%m/%Y") + pd.to_timedelta(df['hour'], unit='h')

    except ValueError:

        if data_type == "CDR":
            df = pd.read_csv(antenna_location)
            df=df.rename(columns={"CALLER_CALLE_SEGMENT":"segment_caller_callee", "DAY":"date", "HOUR":"hour", "CALLEE_MATCHER":"site_id_callee",\
                                  "CALER_MATCHER":"site_id_caller", "SUM(CALL_DURATION)":"call_duration", "CALLER_CALLE_SEGMENT_T":"call_count"})
            df['time'] = pd.to_datetime(df['date'], errors='coerce') + pd.to_timedelta(df['hour'], unit='h')
            df = df.dropna(subset=['time'])
    return df


def antenna_aggregate_calls(df, tower_location, data_direction, spatial_aggregation_level, spatial_aggregation_type,
                            temporal_aggregation_level='week', segment=True, exclude_same_location_calls=False):
    df_processed = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df_processed['time']):
        df_processed['time'] = pd.to_datetime(df_processed['time'])
    df_processed = df_processed[~((df_processed['site_id_caller'] == 0) & (df_processed['site_id_callee'] == 0))]

    valid_types = ['location_to_location', 'per_location']
    if spatial_aggregation_type not in valid_types:
        raise ValueError(f"Invalid value for site_aggregation_type. It should be one of {valid_types}.")

    suffix, opposite_suffix = get_suffixes_based_on_direction(data_direction)

    loc_level = get_location_level(spatial_aggregation_level)
    if spatial_aggregation_type == 'per_location':
        if not spatial_aggregation_level == "site":
            spatial_column = f"{loc_level}_{suffix}"
            df_tower = read_tower_data(tower_location)[['site_id', loc_level]]
            df_processed = df_processed.merge(df_tower, right_on='site_id', left_on=f'site_id_{suffix}', how='left'). \
                rename(columns={loc_level: spatial_column}).drop(columns=['site_id'])
        else:
            spatial_column = f"{loc_level}_{suffix}"
    else:
        if not spatial_aggregation_level == "site":
            spatial_column = f"{loc_level}_{suffix}"
            opposite_spatial_column = f"{loc_level}_{opposite_suffix}"
            df_tower = read_tower_data(tower_location)[['site_id', loc_level]]
            df_processed = df_processed.merge(df_tower, right_on='site_id', left_on=f'site_id_{suffix}', how='left'). \
                rename(columns={loc_level: spatial_column}).drop(columns=['site_id'])
            df_processed = df_processed.merge(df_tower, right_on='site_id', left_on=f'site_id_{opposite_suffix}',
                                              how='left'). \
                rename(columns={loc_level: opposite_spatial_column}).drop(columns=['site_id'])
        else:
            spatial_column = f"{loc_level}_{suffix}"
            opposite_spatial_column = f"{loc_level}_{opposite_suffix}"

    if exclude_same_location_calls and spatial_aggregation_type == 'location_to_location':
        df_processed = df_processed[df_processed[f"{loc_level}_caller"] != df_processed[f"{loc_level}_callee"]]

    df_processed = apply_temporal_aggregation(df_processed, temporal_aggregation_level)

    aggregation = {
        'call_count': 'sum',
        'call_duration': 'sum'
    }
    if spatial_aggregation_type == 'per_location':
        group_by_columns = [temporal_aggregation_level, spatial_column]
    else:
        group_by_columns = [temporal_aggregation_level, spatial_column, opposite_spatial_column]
    if segment:
        group_by_columns.append('segment_caller_callee')
    final_agg = df_processed.groupby(group_by_columns).agg(aggregation).reset_index()

    return final_agg


def dataframe_length_calculator(df):
    length1=len(df["time"].unique())*len(df["site_id_callee"].unique())*len(df["site_id_caller"].unique())
    length2=len(df)
    length3=length2-length1*2
    return print(length1,length2,length3)

def antenna_variable_histogram(df):
    # Check and print the count of zeros and nulls in each column
    for column in df.columns:
        zeros = (df[column] == 0).sum() if df[column].dtype != 'object' else 'N/A'
        nulls = df[column].isnull().sum()
        print(f"Column '{column}': Zeros = {zeros}, Nulls = {nulls}")

    histogram_columns = ['segment_caller_callee', 'site_id_caller', 'site_id_callee',
                         'call_duration', 'call_count']

    # Create histograms
    for column in histogram_columns:
        if column in df.columns:
            plt.figure(figsize=(10, 6))
            if df[column].dtype == 'object':
                df[column].value_counts().plot(kind='bar', title=f"Frequency of {column}")
            else:
                df[column].plot(kind='hist', bins=50, title=f"Histogram of {column}")
            plt.show()

def mean_antenna_usage(antenna_location, data_type, period, n1, n2):

    df = read_antenna_data(antenna_location, data_type)
    #tower = read_tower_data(tower_location)
    #df = df.merge(tower, on='site_id', how='left')
    df = df[(df['time'].dt.hour >= n1) | (df['time'].dt.hour <= n2)].reset_index(drop=True)
    df = df.groupby([df['time'].dt.period.rename(period), df['site_id']]).mean().round(
        decimals=0).reset_index()
    return df

def xdr_migrant_sum(df):
    df['total_migrant']= df['segment_1'] + df['segment_2'] + df['segment_3'] + df['segment_4'] + \
    df['segment_5'] + df['segment_6'] + df['segment_7'] + df['segment_8'] + df['segment_9'] + \
    df['segment_10'] + df['segment_11'] + df['segment_12']
    df['total_locals'] = df['segment_13'] + df['segment_14']
    df['total_refugee'] = df['segment_1'] + df['segment_12']
    return df


def filter_site_by_region(df, list_regions=[]):
    df_tower = read_tower_data(cell_towers)
    df = df.merge(df_tower, on=['site_id'], how='left').reset_index(drop=True)
    df = df[df['city'].isin(list_regions) == True].reset_index(drop=True)
    # df=df[(df['city']=='ISTANBUL')|(df['city']=='KOCAELI')]
    df = df.drop(columns=['city', 'district'])

    return df

def calculate_time_variables(df):
    df['day']=df['time'].dt.day
    df['month']=df['time'].dt.month
    df['hour']=df['time'].dt.hour
    df['day_of_the_week']=df['time'].dt.dayofweek
    df['weekday'] = np.where(df['day_of_the_week'] < 5,0,1)
    df['weekend'] = np.where(df['day_of_the_week'] >= 5,0,1)
    return df


def filter_time_variables(df, list_days=[], list_hours=[]):
    # check the data types
    if type(list_hours[0]) == int:
        list_hours = [int(x) for x in list_hours]
    else:
        pass
    if type(list_days[0]) == str:
        list_days = [str(x) for x in list_days]
    else:
        pass

    df['month-day'] = df['month'].astype('str') + '-' + df['day'].astype('str')
    # df=df[df['hour']!=23].reset_index(drop=True)
    df = df[df['hour'].isin(list_hours) == False].reset_index(drop=True)
    df = df[df['month-day'].isin(list_days) == False].reset_index(drop=True)

    print("Antenna data is filtered...\n")
    return df

def antenna_group_summary(antenna_location, data_type):
    df = read_antenna_data(antenna_location,data_type)
    #df = xdr_migrant_sum(df)
    df['hour'] = df['time'].dt.hour
    df['month'] = df['time'].dt.month
    df['day'] = df['time'].dt.day
    df['year'] = df['time'].dt.year
    df=df[df['hour']!=23]
    df_1 = df[(df['time'].dt.hour > 0) & (df['time'].dt.hour <= 6)].reset_index(drop=True)
    df_1['group']=1
    df_2 = df[(df['time'].dt.hour > 6) & (df['time'].dt.hour <= 12)].reset_index(drop=True)
    df_2['group']=2
    df_3 = df[(df['time'].dt.hour > 12) & (df['time'].dt.hour <= 18)].reset_index(drop=True)
    df_3['group']=3
    df_4 = df[(df['time'].dt.hour > 18) | (df['time'].dt.hour == 0)].reset_index(drop=True)
    df_4['group']=4
    #print(df_4.head())
    print('Grouping starts ...')
    group_1 = df_1.groupby([df_1['day'],df_1['site_id']]).mean().round(decimals=0).reset_index()
    group_2 = df_2.groupby([df_2['day'],df_2['site_id']]).mean().round(decimals=0).reset_index()
    group_3 = df_3.groupby([df_3['day'],df_3['site_id']]).mean().round(decimals=0).reset_index()
    group_4 = df_4.groupby([df_4['day'],df_4['site_id']]).mean().round(decimals=0).reset_index()
    new_frame = pd.concat([group_1, group_2, group_3, group_4]).reset_index(drop=True)
    print(antenna_location, "is completed.")
    return new_frame


def annual_mean_antenna(df, cdr=False, totals=False):
    # segs=['segment_1','segment_2','segment_3','segment_4','segment_5',\
    #      'segment_6','segment_7','segment_8','segment_9','segment_10',\
    #      'segment_11','segment_12','segment_13','segment_14']
    # segs_total=['segment_1','segment_2','segment_3','segment_4','segment_5',\
    #      'segment_6','segment_7','segment_8','segment_9','segment_10',\
    #      'segment_11','segment_12','segment_13','segment_14','total_locals',\
    #           'total_migrant','total_refugee','total']
    segs_cdr = ['NUMBER_OF_CALLS', 'NUMBER_OF_REFUGEE_CALLS']
    segs = df.columns[(df.columns.str.contains('segment')) & (~df.columns.str.contains('mean'))].tolist()
    segs_total = df.columns[((df.columns.str.contains('segment')) | \
                             (df.columns.str.contains('total'))) &
                            (~df.columns.str.contains('mean'))].tolist()
    if cdr == True:
        v = [df[i].groupby(df['site_id']).mean() for i in segs_cdr]
    else:
        if totals == False:
            v = [df[i].groupby(df['site_id']).mean() for i in segs]
        else:
            v = [df[i].groupby(df['site_id']).mean() for i in segs_total]
    df_v = [pd.DataFrame(v[i]) for i in range(len(v))]
    df_v = pd.concat(df_v).reset_index()
    df_v = df_v.groupby('site_id', as_index=False).first()
    df_v = df_v.add_suffix('_mean')
    df_v = df_v.rename({'site_id_mean': 'site_id'}, axis=1)
    df = df.merge(df_v, how='outer').ffill()
    return df


def annual_max_antenna(df, cdr=False, totals=False, peak_coefficient=0):
    if df.columns.str.contains('max').any() == True:
        df = df[df.columns.drop(list(df.filter(regex='max')))]

    if df.columns.str.contains('mean').any() == False:
        df = annual_mean_antenna(df)

    for i in segs:
        # df['max_'+i]=df[i][(df[i+str('_mean')]*peak_coefficient < df[i])]
        df['max_' + i] = df.index.isin(df.loc[df[i + str('_mean')] * peak_coefficient < df[i], i].index)

    return df


def annual_min_antenna(df, cdr=False, totals=False, peak_coefficient=0):
    if df.columns.str.contains('min').any() == True:
        df = df[df.columns.drop(list(df.filter(regex='max')))]

    if df.columns.str.contains('mean').any() == False:
        df = annual_mean_antenna(df)

    for i in segs:
        # df['max_'+i]=df[i][(df[i+str('_mean')]*peak_coefficient < df[i])]
        df['min_' + i] = df.index.isin(df.loc[df[i + str('_mean')] / peak_coefficient > df[i], i].index)

    return df


def filter_antenna(df, segment='segment_1', val=1):
    the_list = df[df[segment + '_mean'] > np.percentile(df[segment + '_mean'], val)].site_id.unique().tolist()
    sites = ([(x, y) for x in the_list for y in the_list if x != y if x < y])

    return sites, the_list


def correlation_between_sites(df, segment='segment_1', val=1):
    sites, the_list = filter_antenna(df, segment='segment_1', val=1)
    site_pivot = pd.pivot_table(df[df[segment + '_mean'] > np.percentile(df[segment + '_mean'], val)], values=[segment],
                                columns=['site_id'], index=['time']
                                )
    site_pivot.columns = site_pivot.columns.droplevel(0)
    site_pivot.columns = the_list
    correlation_pts = [site_pivot[[x, y]].corr().reset_index().iloc[:, 1][1] for x, y in sites]

    df_corr = list(zip(correlation_pts, sites))
    df_corr = pd.DataFrame(df_corr, columns=['corr', 'sites'])

    df_corr[['site_id1', 'site_id2']] = pd.DataFrame(df_corr['sites'].tolist(), index=df_corr.index)
    df_corr = df_corr[['corr', 'site_id1', 'site_id2']]

    df_corr_pivot = pd.pivot_table(df, index=['city', 'site_id', 'district']).reset_index()
    df_corr_pivot = df_corr_pivot[['city', 'site_id', 'district']]

    df_corr = df_corr_pivot.merge(df_corr, how='right', right_on='site_id1', left_on='site_id')
    df_corr = df_corr.rename({'city': 'city1', 'district': 'district1'}, axis=1)
    df_corr = df_corr.drop(['site_id'], axis=1)

    df_corr = df_corr_pivot.merge(df_corr, how='right', right_on='site_id2', left_on='site_id')
    df_corr = df_corr.rename({'city': 'city2', 'district': 'district2'}, axis=1)
    return df_corr


def read_merge_antenna_flows(is_repeat, omit=True):
    print("Reading antenna data...\n")
    df_trans = pd.DataFrame()
    for f in os.listdir(location):
        if 'E_Cell_tower_usage_data_2020' in f:
            df_trans = df_trans.append(pd.read_csv(os.path.join(location, f),
                                                   sep="|", skiprows=0,
                                                   header=0, encoding='ISO-8859-1'))
        print("Dataset" + str(f) + " is read")

    df_trans = df_trans.drop(['Unnamed: 0', 'Unnamed: 17'], axis=1)
    df_trans = df_trans.iloc[1:, :]
    df_trans = df_trans.replace('                    ', 0)
    df_trans = df_trans.reset_index(drop=True)
    df_trans = df_trans.rename(columns=lambda x: x.strip())
    df_trans = df_trans[df_trans['segment_2'] != "--------------------"]
    df_trans[df_trans.columns[1:16]] = df_trans[df_trans.columns[1:16]].astype(int)
    tower = pd.read_csv(
        r"/Volumes/Extreme SSD/Data - Location/Hummingbird_Location_Datas/A_Cell_Tower_Locations/cell_city_district.txt",
        sep="|",
        header=0, encoding='ISO-8859-1')
    tower = tower.drop(['Unnamed: 0', 'Unnamed: 4'], axis=1)
    tower = tower.iloc[1:, :]
    tower = tower.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    tower = tower.rename(columns=lambda x: x.strip())
    tower = tower.rename(columns={'matcher': 'site_id'})
    tower['site_id'] = tower['site_id'].astype(int)
    df_trans = df_trans.merge(tower, on='site_id', how='left')
    print('There are {} cell towers in the dataset'.format(df_trans.site_id.nunique()))

    return df_trans





def read_cdr_antenna_data(file_directory_antenna_cdr):
    df = pd.read_csv(file_directory_antenna_cdr, sep=',', encoding="ISO-8859-1", error_bad_lines=False)
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], infer_datetime_format=True)
    return df

def merge_cdr_antenna_tower_data(df, file_directory_towers):
    df_tower = pd.read_csv(file_directory_towers, sep=",", header=0)
    df_tower = df_tower[['BTS_ID', 'MX_SAHAIL', 'MX_SAHAILCE']]
    df_tower = df_tower.rename({'BTS_ID': 'OUTGOING_SITE_ID'}, axis=1)
    df = df.merge(df_tower, on=['OUTGOING_SITE_ID'], how='left')
    df = df.rename({'MX_SAHAIL':'MX_SAHAIL_OUT', 'MX_SAHAILCE':'MX_SAHAILCE_OUT'}, axis=1)
    df_tower = df_tower.rename({'OUTGOING_SITE_ID': 'INCOMING_SITE_ID'}, axis=1)
    df = df.merge(df_tower, on=['INCOMING_SITE_ID'], how='left')
    df = df.rename({'MAX_SAHAIL':'MX_SAHAIL_IN', 'MX_SAHAILCE':'MX_SAHAILCE_IN'}, axis=1)

    return df


def feature_extraction_stat(df):
    segs_total=['segment_1','segment_2','segment_3','segment_4','segment_5',\
          'segment_6','segment_7','segment_8','segment_9','segment_10',\
          'segment_11','segment_12','segment_13','segment_14','total_locals',\
               'total_migrant','total_refugee','total']
    v = [df.groupby(['site_id','hour','weekday']).agg({i: ['mean', 'min', 'max','sum']}) for i in segs_total]

    df_v = [pd.DataFrame(v[i]) for i in range(len(v))]
    df_v=pd.concat(df_v).reset_index()
    df_v=df_v.groupby(['site_id','hour','weekday'], as_index=False).first()
    return df_v


def antenna_summed(df):
    ###### BURADA ŞEHRE GORE TOPLUYORSUN SADECE!
    ###### NEYE GORE TOPLADIGIN ONEMLI!

    df_aggregate = pd.pivot_table(df, index=['city', 'month', 'day', 'year'], \
                                  aggfunc=np.sum).reset_index()

    totals = ['total_refugee', 'total_locals']
    list_1 = [i + str('_summed') for i in segs]
    list_2 = [i + str('_summed') for i in totals]
    list_3 = ['city', 'month', 'day', 'year']

    df_aggregate_1 = df_aggregate[segs].set_axis(list_1, axis=1)
    df_aggregate_2 = df_aggregate[totals].set_axis(list_2, axis=1)
    df_aggregate_3 = df_aggregate[list_3]

    del (df_aggregate)
    df_aggregate = pd.concat([df_aggregate_1, df_aggregate_2, df_aggregate_3], axis=1)
    # df_aggregate = df_aggregate.drop(['site_id_summed'], axis = 1)
    df = df.merge(df_aggregate, on=['city', 'month', 'day', 'year'], how='left')
    return df

def get_mean_frames(antenna_location,data_type):
    df = read_antenna_data(antenna_location,data_type)
    df = xdr_migrant_sum(df)
    df['month'] = df['time'].dt.month
    df['day'] = df['time'].dt.day
    df['year'] = df['time'].dt.year
    df_night = df[((df['time'].dt.hour > 20) & (df['time'].dt.hour != 23)) | (df['time'].dt.hour < 8)].reset_index(drop=True)
    group_night = df_night.groupby([df_night['time'].dt.day.rename('nightly_mean'),df_night['site_id']]).mean().round(decimals=0).reset_index()
    df_day = df[(df['time'].dt.hour <= 20) | (df['time'].dt.hour >= 8)].reset_index(drop=True)
    group_day = df_day.groupby([df_day['time'].dt.day.rename('daily_mean'),df_day['site_id']]).mean().round(decimals=0).reset_index()
    df_23 = df[(df['time'].dt.hour == 23)].reset_index(drop=True)
    group_23 = df_23.groupby([df_23['time'].dt.day.rename('At_23'),df_23['site_id']]).mean().round(decimals=0).reset_index()
    new_frame = pd.concat([group_day, group_night, group_23]).reset_index(drop=True)
    return group_day, group_night, group_23
