import pandas as pd
import numpy as np
import os
import math
import sys

dir_finegrained = "/Extreme SSD/Data - Location/Hummingbird_Location_Data/F_Fine_grained_mobility"

dir_summary = "/Extreme SSD/Summary_Data/Fine grained/"

summary_file = "fine_grained{}.csv"

user_input = input("Enter the operating system type: (mac or linux) ")

if user_input.lower() == "linux":
    var_os_type = "linux"
elif user_input.lower() == "mac":
    var_os_type = "mac"
else:
    print("Invalid input. Setting the OS type as 'Unknown'.")
    var_os_type = "Unknown"

# Rest of your code...

def read_tower_data(os_type):
    dir_tower = "/Extreme SSD/Data - Location/Hummingbird_Location_Data/A_Cell_Tower_Locations/cell_city_district.txt"
    
    tower_location = ostype_add_directory(os_type,dir_tower)
    
    tower = pd.read_csv(
        tower_location,
        sep="|",
        header=0, encoding='ISO-8859-1')
    tower = tower.drop(['Unnamed: 0', 'Unnamed: 4'], axis=1)
    tower = tower.iloc[1:, :]
    tower = tower.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    tower = tower.rename(columns=lambda x: x.strip())
    tower = tower.rename(columns={'matcher': 'site_id'})
    tower['site_id'] = tower['site_id'].astype(int)
    #print('There are {} cell towers in the dataset'.format(df.site_id.nunique()))
    return tower

def read_fine_grained(os_type,dir_finegrained,file_number):
    
    finegrained_file = "/fine_grained{}.txt"
    
    fine_grained_location=ostype_add_directory(os_type,dir_finegrained)+ finegrained_file.format(file_number)
    
    df = pd.read_csv(fine_grained_location,
                     sep="|", skiprows=0,
                     header=0, encoding='ISO-8859-1')
    df = df.drop(['Unnamed: 0', 'Unnamed: 5'], axis=1)
    df = df.rename(columns=lambda x: x.strip())
    for i in df.columns:
        df[i] = df[i].astype(str)
    df = df.apply(lambda x: x.str.strip())
    df = df.iloc[1:, :]

    for i in df.columns[1:4]:
        df[i] = df[i].astype(int)        
    df = infer_datetime(df,file_number)
    
    return df

def infer_datetime(df,file_number):
    # get the expected month from the first entry
    expected_month = math.ceil(file_number/2)

    # try to convert the first entry of time with the specified format
    try:
        dt = pd.to_datetime(df.iloc[0,0], format='%Y-%m-%d %H')
        if dt.month == expected_month:
            print('Datetime format is correct')
            df['time'] = pd.to_datetime(df['time'], format='%Y-%m-%d %H')
        else:
            raise ValueError
    except ValueError:
        # if it fails, then try automatic inference
        print('Datetime format is incorrect, trying automatic inference')
        try:
            dt = pd.to_datetime(df.iloc[0,0])
            if dt.month == expected_month:
                print('Automatic inference is successful')
                df['time'] = pd.to_datetime(df['time'])
            else:
                raise ValueError
        except ValueError:
            print('Automatic inference failed')

    return df


def customer_signals_analysis(df):
    df['time'] = pd.to_datetime(df['time'])
    df['hour'] = df['time'].dt.hour
    df['day_of_week'] = df['time'].dt.dayofweek
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
    df['is_night'] = df['hour'].isin(range(18, 24)) | df['hour'].isin(range(0, 7)) 
    df['is_day'] = (df['hour'].isin(range(7,18))).astype(int)
    df['is_23'] = (df['hour'] == 23).astype(int)
    df['is_other_times'] = (df['hour'] != 23).astype(int)
    df['day'] = df['time'].dt.date
    df['site_count'] = df.groupby('customer_id')['site_id'].transform('nunique')
    df['day_count'] = df.groupby('customer_id')['day'].transform('nunique')

    customers_analysis = df.groupby('customer_id').agg({
        'time': 'count',
        'is_23': 'sum',
        'is_other_times': 'sum',
        'is_weekend': 'sum',
        'is_night': 'sum',
        'is_day': 'sum',
        'day_count': 'first',
        'site_count': 'first'
    }).rename(columns={
        'time': 'signal_count',
        'is_23': 'signal_at_23',
        'is_other_times': 'signal_at_other_times',
        'is_weekend': 'signal_on_weekend',
        'is_night': 'signal_at_night',
        'is_day': 'signal_during_day',
        'day_count': 'unique_days_count',
        'site_count': 'unique_sites_count'
    })

    return customers_analysis.reset_index()

def filter_customers(cust_df, unique_days_threshold, signal_23_ratio_threshold, weekend_signal_threshold, night_signal_threshold, day_signal_threshold):
    filtered_df = cust_df[
        (cust_df['unique_days_count'] >= unique_days_threshold) &
        (cust_df['signal_at_23'] / cust_df['signal_count'] <= signal_23_ratio_threshold) &
        (cust_df['signal_on_weekend'] > weekend_signal_threshold) &
        (cust_df['signal_at_night'] > night_signal_threshold) &
        (cust_df['signal_during_day'] > day_signal_threshold)
    ]
    return filtered_df

def ostype_add_directory(os_type, directory):
    base_dir_mac = "/Volumes"
    base_dir_linux = "/media/f140926"
    base_dir = base_dir_mac if os_type.lower() == 'mac' else base_dir_linux
    based_directory = base_dir + directory 
    return based_directory
    

harvest_cities=['GIRESUN','ORDU','TRABZON']#'RIZE'
df_tower= read_tower_data(var_os_type)

for i in range(19, 21):
    print(i,'start')
    #fine_name = finegrained_file.format(i)
    #fine_path = os.path.join(dir_finegrained, fine_name)
    sum_path = ostype_add_directory(var_os_type, dir_summary)+summary_file.format(i)
    print(sum_path)
    df_summary = pd.read_csv(sum_path)[['customer_id','city']]
    
    
    df_finegrained= read_fine_grained(var_os_type,dir_finegrained,i)
    cust_df = customer_signals_analysis(df_finegrained)
    filtered_df = filter_customers(cust_df, unique_days_threshold=10, signal_23_ratio_threshold=0.5, weekend_signal_threshold=5, night_signal_threshold=10, day_signal_threshold=10)
    df_finegrained=df_finegrained[df_finegrained['customer_id'].isin(filtered_df['customer_id'].unique().tolist())==True].reset_index(drop=True)
    del(cust_df)
    del(filtered_df)
    
    
    cust_list_ist=df_summary[df_summary['city']=='ISTANBUL'].reset_index(drop=True)['customer_id'].unique().tolist()
    cust_list_giresun=df_summary[df_summary['city']=='GIRESUN'].reset_index(drop=True)['customer_id'].unique().tolist()
    cust_list_ordu=df_summary[df_summary['city']=='ORDU'].reset_index(drop=True)['customer_id'].unique().tolist()
    cust_list_trabzon=df_summary[df_summary['city']=='TRABZON'].reset_index(drop=True)['customer_id'].unique().tolist()

    cust_list_destination=cust_list_ordu+cust_list_giresun+cust_list_trabzon

    df_finegrained=df_finegrained.merge(df_tower,on='site_id',how='left')

    df_finegrained_ist=df_finegrained[df_finegrained['customer_id'].isin(cust_list_ist)==True].reset_index(drop=True)
    customer_ids_of_seasonal_migrants_1=df_finegrained_ist[df_finegrained_ist['city'].isin(harvest_cities)==True]['customer_id'].unique().tolist()


    df_finegrained_destination=df_finegrained[df_finegrained['customer_id'].isin(cust_list_destination)==True].reset_index(drop=True)
    customer_ids_of_seasonal_migrants_2=df_finegrained_destination[df_finegrained_destination['city'].isin(['ISTANBUL'])==True]['customer_id'].unique().tolist()

    potential_seasonal_migrants=customer_ids_of_seasonal_migrants_1+customer_ids_of_seasonal_migrants_2
    cust_list_nonmovers = [item for item in cust_list_ist if item not in potential_seasonal_migrants]
    df_potential_seasonal_migrants = pd.DataFrame(potential_seasonal_migrants, columns=['customer_id'])
    df_nonmovers= pd.DataFrame(cust_list_nonmovers, columns=['customer_id'])
    
    dir_to_save_harvest=ostype_add_directory(var_os_type,'/Extreme SSD/MPD_based_indicators_of_migration/Seasonal_migration/harvest_trips/harvest_trips_2020_{}.csv')
    dir_to_save_nonmovers=ostype_add_directory(var_os_type,'/Extreme SSD/MPD_based_indicators_of_migration/Seasonal_migration/harvest_trips/non_movers_2020_{}.csv')
    
    df_potential_seasonal_migrants.to_csv(dir_to_save_harvest.format(i))
    df_nonmovers.to_csv(dir_to_save_nonmovers.format(i))
    
    print(i,'done')

#holiday_cities=['BALIKESIR','MUGLA','IZMIR','ANTALYA','CANAKKALE']

