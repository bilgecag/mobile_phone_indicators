import pandas as pd
import numpy as np
import os
import math
import sys
from functions.helper.tower_helper import read_tower_data
from functions.helper.finegrained_helper import read_fine_grained,customer_signals_analysis,filter_customers
from functions.helper.general_helper import ostype_add_directory

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


holiday_cities=['BALIKESIR','MUGLA','IZMIR','ANTALYA','CANAKKALE']

df_tower= read_tower_data(var_os_type)

for i in range(21, 22):
    print(i,'start')
    #fine_name = finegrained_file.format(i)
    #fine_path = os.path.join(dir_finegrained, fine_name)
    
    sum_path = ostype_add_directory(var_os_type, dir_summary)+summary_file.format(i)

    
    df_finegrained= read_fine_grained(var_os_type,dir_finegrained,i)
    cust_df = customer_signals_analysis(df_finegrained)
    filtered_df = filter_customers(cust_df, unique_days_threshold=10, signal_23_ratio_threshold=0.5, weekend_signal_threshold=5, night_signal_threshold=10, day_signal_threshold=10)
    df_finegrained=df_finegrained[df_finegrained['customer_id'].isin(filtered_df['customer_id'].unique().tolist())==True].reset_index(drop=True)
    del(cust_df)
    del(filtered_df)

    
    df_summary = pd.read_csv(sum_path)[['customer_id','city']]

    cust_list_ist=df_summary[df_summary['city']=='ISTANBUL'].reset_index(drop=True)['customer_id'].unique().tolist()
    cust_list_balikesir=df_summary[df_summary['city']=='BALIKESIR'].reset_index(drop=True)['customer_id'].unique().tolist()
    cust_list_mugla=df_summary[df_summary['city']=='MUGLA'].reset_index(drop=True)['customer_id'].unique().tolist()
    cust_list_izmir=df_summary[df_summary['city']=='IZMIR'].reset_index(drop=True)['customer_id'].unique().tolist()
    cust_list_antalya=df_summary[df_summary['city']=='ANTALYA'].reset_index(drop=True)['customer_id'].unique().tolist()
    cust_list_canakkale=df_summary[df_summary['city']=='CANAKKALE'].reset_index(drop=True)['customer_id'].unique().tolist()


    cust_list_destination=cust_list_balikesir+cust_list_mugla+cust_list_izmir+cust_list_antalya+cust_list_canakkale

    df_finegrained=df_finegrained.merge(df_tower,on='site_id',how='left')

    df_finegrained_ist=df_finegrained[df_finegrained['customer_id'].isin(cust_list_ist)==True].reset_index(drop=True)
    customer_ids_of_seasonal_migrants_1=df_finegrained_ist[df_finegrained_ist['city'].isin(holiday_cities)==True]['customer_id'].unique().tolist()


    df_finegrained_destination=df_finegrained[df_finegrained['customer_id'].isin(cust_list_destination)==True].reset_index(drop=True)
    customer_ids_of_seasonal_migrants_2=df_finegrained_destination[df_finegrained_destination['city'].isin(['ISTANBUL'])==True]['customer_id'].unique().tolist()

    potential_seasonal_migrants=customer_ids_of_seasonal_migrants_1+customer_ids_of_seasonal_migrants_2

    cust_list_nonmovers = [item for item in cust_list_ist if item not in potential_seasonal_migrants]

    df_potential_seasonal_migrants = pd.DataFrame(potential_seasonal_migrants, columns=['customer_id'])#cust_list_ist = potential_seasonal_migrants
    df_nonmovers = pd.DataFrame(cust_list_nonmovers, columns=['customer_id'])
    
    dir_to_save_holiday=ostype_add_directory(var_os_type,"/Extreme SSD/MPD_based_indicators_of_migration/seasonal_migration/data/holiday_trips_2020_{}.csv")
    dir_to_save_nonmover=ostype_add_directory(var_os_type,"/Extreme SSD/MPD_based_indicators_of_migration/seasonal_migration/data/non_movers_2020_{}.csv")
    df_potential_seasonal_migrants.to_csv(dir_to_save_holiday.format(i))
    df_potential_seasonal_migrants.to_csv(dir_to_save_nonmover.format(i))
    print(i,'done')


