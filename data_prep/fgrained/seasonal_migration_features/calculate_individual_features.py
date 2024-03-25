import pandas as pd
import os
import sys

from indicators.helper_functions.finegrained_helper import (
    read_fine_grained,
    customer_signals_analysis,
    filter_customers,
    filter_fgrained_by_vars)

from indicators.helper_functions.general_helper import ostype_add_directory
from indicators.mobility_indicators.mobility_indicators import (
    calculate_bandicoot_indicators,
    travel_info,
    calculate_scikit_indicators
    )

#user_input = input("Enter the operating system type: (mac or linux) ")
#print(sys.argv)
user_input = sys.argv[1]
i = sys.argv[2]

if user_input.lower() == "linux":
    var_os_type = "linux"
elif user_input.lower() == "mac":
    var_os_type = "mac"
else:
    print("Invalid input. Setting the OS type as 'Unknown'.")
    var_os_type = "Unknown"

            
######## SCIKIT MOBILITY


dir_finegrained="/Extreme SSD/Data - Location/Hummingbird_Location_Data/F_Fine_grained_mobility"
dir_harvest= ostype_add_directory(var_os_type,"/Extreme SSD/MPD_based_indicators_of_migration/seasonal_migration/data/labels/harvest_labels/")
dir_holiday= ostype_add_directory(var_os_type,"/Extreme SSD/MPD_based_indicators_of_migration/seasonal_migration/data/labels/holiday_labels/")
nonmover_file="non_movers_2020_{}.csv"
harvest_file="harvest_trips_2020_{}.csv"
holiday_file="holiday_trips_2020_{}.csv"
holiday_cities=['BALIKESIR','MUGLA','IZMIR','ANTALYA','CANAKKALE']
harvest_cities=['GIRESUN','ORDU','TRABZON']
tower_location = ostype_add_directory(var_os_type,'/Extreme SSD/MPD_based_indicators_of_migration/seasonal_migration/data/towers/towers.csv')


####### PROCESSING #########

    
harvest_name = harvest_file.format(i)
harvest_path = os.path.join(dir_harvest,harvest_name)
holiday_name = holiday_file.format(i)
holiday_path = os.path.join(dir_holiday,holiday_name)
nonmover_name = nonmover_file.format(i)
nonmover_holiday_path = os.path.join(dir_holiday,nonmover_name)
nonmover_harvest_path = os.path.join(dir_harvest,nonmover_name)

df_harvest=pd.read_csv(harvest_path)
print("Harvest customer list is read for",i)
df_holiday=pd.read_csv(holiday_path)
print("Holiday customer list is read for",i)
df_nonmover_holiday=pd.read_csv(nonmover_holiday_path)
df_nonmover_harvest=pd.read_csv(nonmover_harvest_path)
df_customers=pd.concat([df_harvest,df_nonmover_harvest,df_nonmover_holiday,df_holiday]).drop_duplicates(['customer_id'])
#df_customers=pd.concat([df_harvest,df_nonmover_harvest]).drop_duplicates(['customer_id'])
list_customers=df_customers['customer_id'].tolist()
del (df_customers, df_nonmover_holiday,df_nonmover_harvest,df_holiday,df_harvest)
print("Customer list is ready for data set number",i)
print("There are", len(list_customers)," number of customers.")

df_finegrained= read_fine_grained(var_os_type,dir_finegrained,i)
print('Read is done.')
cust_df = customer_signals_analysis(df_finegrained)
print('Customer_signals_analysis is done.')
filtered_df_custs = filter_customers(cust_df, signal_count = 20, unique_days_threshold=10, signal_23_ratio_threshold=0.5, weekend_signal_threshold=5,night_signal_threshold=10)['customer_id'].unique().tolist()
print('Filter_customers function is ran.')
df_finegrained=df_finegrained[df_finegrained['customer_id'].isin(filtered_df_custs)==True].reset_index(drop=True)
print("Fine grained is filtered.")
del(cust_df,filtered_df_custs)
    
# TRAVEL INDICATORS
    
print("Travel indicators are being calculated...")

df_finegrained_travel = travel_info(filter_fgrained_by_vars(df_finegrained, 23, list_customers, ['ISTANBUL','KOCAELI'], exclude_hour=True,include_city_list=False),site_to_city, site_to_district, site_to_lat, site_to_long)

df_finegrained_travel.to_csv(ostype_add_directory(var_os_type,'/Extreme SSD/MPD_based_indicators_of_migration/seasonal_migration/data/features/individual_mobility_indicators/travel_indicators_{}.csv').format(i))

print(df_finegrained_travel.head(5))
del(df_finegrained_travel)


# BANDICOOT INDICATORS
df_finegrained_filtered=filter_fgrained_by_vars(df_finegrained, 23, list_customers, ['ISTANBUL', 'KOCAELI'], exclude_hour=True,include_city_list=True)
del (df_finegrained)
df_finegrained_filtered_bandicoot=calculate_bandicoot_indicators(df_finegrained_filtered)
print(df_finegrained_filtered_bandicoot.head(5))
df_finegrained_filtered_bandicoot.to_csv(ostype_add_directory(var_os_type,'/Extreme SSD/MPD_based_indicators_of_migration/seasonal_migration/data/features/individual_mobility_indicators/bandicoot_indicators_{}.csv').format(i))
del(df_finegrained_filtered_bandicoot)

# SCIKIT-MOBILITY INDICATORS

df_week= df_finegrained_filtered[df_finegrained_filtered['is_weekend']==0].reset_index(drop=True)
df_weekend=df_finegrained_filtered[df_finegrained_filtered['is_weekend']==1].reset_index(drop=True)
df_night=df_finegrained_filtered[df_finegrained_filtered['night_dummy']==1].reset_index(drop=True)
df_day=df_finegrained_filtered[df_finegrained_filtered['night_dummy']==0].reset_index(drop=True)
del(df_finegrained_filtered)
    
df_finegrained_filtered_scikit_week=calculate_scikit_indicators(df_week, site_to_lat,site_to_long)
df_finegrained_filtered_scikit_week.to_csv(ostype_add_directory(var_os_type,'/Extreme SSD/MPD_based_indicators_of_migration/seasonal_migration/data/features/individual_mobility_indicators/scikit_indicators_week_{}.csv').format(i))
    
del(df_finegrained_filtered_scikit_week,df_week)
df_finegrained_filtered_scikit_weekend=calculate_scikit_indicators(df_weekend, site_to_lat,site_to_long)
df_finegrained_filtered_scikit_weekend.to_csv(ostype_add_directory(var_os_type,'/Extreme SSD/MPD_based_indicators_of_migration/seasonal_migration/data/features/individual_mobility_indicators/scikit_indicators_weekend_{}.csv').format(i))
del(df_finegrained_filtered_scikit_weekend,df_weekend)
df_finegrained_filtered_scikit_night=calculate_scikit_indicators(df_night, site_to_lat,site_to_long)
df_finegrained_filtered_scikit_night.to_csv(ostype_add_directory(var_os_type,'/Extreme SSD/MPD_based_indicators_of_migration/seasonal_migration/data/features/individual_mobility_indicators/scikit_indicators_night_{}.csv').format(i))
del(df_finegrained_filtered_scikit_night,df_night)
df_finegrained_filtered_scikit_day=calculate_scikit_indicators(df_day, site_to_lat,site_to_long)
df_finegrained_filtered_scikit_day.to_csv(ostype_add_directory(var_os_type,'/Extreme SSD/MPD_based_indicators_of_migration/seasonal_migration/data/features/individual_mobility_indicators/scikit_indicators_day_{}.csv').format(i))
del(df_finegrained_filtered_scikit_day,df_day)
print("Done")


    

        



    
    
