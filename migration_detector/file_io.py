# import graphlab as gl
#import turicreate as gl
import pandas as pd
import numpy as np
import os
from .core import TrajRecord
import pandas as pd
import os
from datetime import timedelta

def read_csv(file_path):
    user_daily_loc_count = pd.read_csv(file_path)
    # Convert 'user_id' to string for pandas processing
    user_daily_loc_count['user_id'] = user_daily_loc_count['user_id'].astype(str)

    start_date_ori = str(user_daily_loc_count['date'].min())
    end_date_ori = str(user_daily_loc_count['date'].max())

    # MM/DD/YYYY
    start_date = pd.to_datetime(start_date_ori, format='%Y%m%d').strftime('%m/%d/%Y')
    end_date = pd.to_datetime(end_date_ori, format='%Y%m%d').strftime('%m/%d/%Y')

    all_date = pd.date_range(start=start_date, end=end_date)
    all_date_new = [int(date.strftime('%Y%m%d')) for date in all_date]
    date2index = dict(zip(all_date_new, range(len(all_date_new))))
    index2date = dict(zip(range(len(all_date_new)), all_date_new))

    end_date_long_ori = str(pd.Timestamp(end_date) + pd.Timedelta('200 days'))
    all_date_long = pd.date_range(start=start_date, end=end_date_long_ori)
    all_date_long_new = [int(date.strftime('%Y%m%d')) for date in all_date_long]
    date_num_long = pd.DataFrame({'date': all_date_long_new, 'date_num': range(len(all_date_long_new))})

    # Adding 'date_num' to migration_df for pandas
    migration_df = user_daily_loc_count.copy()
    migration_df['date_num'] = migration_df['date'].apply(lambda x: date2index[x])

    # Aggregating user data for pandas
    #user_loc_date_agg = migration_df.groupby(['user_id', 'location'])['date_num'].apply(list).reset_index()
    #print(user_loc_date_agg)
    # Aggregating location data for pandas
    #user_loc_agg = user_loc_date_agg.groupby('user_id').apply(
    #    lambda x: to_dict(zip(x.location, x.date_num))).reset_index()

    user_loc_date_agg = migration_df.groupby(['user_id', 'location']).agg({'date_num': lambda x: list(x)}).reset_index()
    user_loc_agg = user_loc_date_agg.groupby('user_id').apply(lambda x: dict(zip(x['location'], x['date_num']))).reset_index(name='all_record')

    user_loc_agg.columns = ['user_id', 'all_record']
    #print(user_loc_agg)
    traj = TrajRecord(user_loc_agg, migration_df, index2date, date_num_long)
    return traj



def to_csv(result, result_path='result', file_name='migration_event.csv'):
    if not os.path.isdir(result_path):
        os.makedirs(result_path)
    save_file = os.path.join(result_path, file_name)
    result.select_columns(
            ['user_id', 'home', 'destination', 'migration_date',
         'uncertainty', 'num_error_day',
         'home_start', 'home_end',
         'destination_start', 'destination_end',
         'home_start_date', 'home_end_date',
         'destination_start_date', 'destination_end_date']
    ).export_csv(save_file)

