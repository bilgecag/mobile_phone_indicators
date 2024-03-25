if __name__ == "__main__":

    import pandas as pd
    import os
    import sys
    from indicators.helper_functions.general_helper import ostype_add_directory

    user_input = sys.argv[1]

    if user_input.lower() == "linux":
        var_os_type = "linux"
    elif user_input.lower() == "mac":
        var_os_type = "mac"
    else:
        print("Invalid input. Setting the OS type as 'Unknown'.")
        var_os_type = "Unknown"


    # Define the directories
    dir_individual_indicators = ostype_add_directory(var_os_type, '/Extreme SSD/MPD_based_indicators_of_migration/seasonal_migration/data/features/individual_mobility_indicators/')
    dir_harvest = ostype_add_directory(var_os_type, '/Extreme SSD/MPD_based_indicators_of_migration/seasonal_migration/data/labels/harvest_labels/')
    neighborhood_indicators_file = ostype_add_directory(var_os_type, '/Extreme SSD/MPD_based_indicators_of_migration/seasonal_migration/data/features/spatial_indicators/neighborhood_indicators.csv')
    dir_home_location = ostype_add_directory(var_os_type, '/Extreme SSD/Summary_Data/Fine grained/')
    

    # Get the list of all files in harvest_dir directory
    all_harvest_files = os.listdir(dir_harvest)
    all_indicator_files = os.listdir(dir_individual_indicators)
    all_home_loc_files = os.listdir(dir_home_location)
    
    # Filter to get only .csv files
    harvest_files = [os.path.join(dir_harvest, file) for file in all_harvest_files if file.endswith('.csv')]
    indicator_files = [os.path.join(dir_individual_indicators, file) for file in all_indicator_files if file.endswith('.csv')]
    home_loc_files = [os.path.join(dir_home_location, file) for file in all_home_loc_files if file.endswith('.csv')]
    
    # Dataframe to hold all merged data

    final_df = pd.DataFrame()
    # Loop over each unique week
    for week in ['15','16','17','18','19','10','11','12', '13','14']:#:
        #weekly_df = pd.DataFrame()
        print(week)
        # Filter harvest_files for the specific week
        week_harvest_files = [file for file in harvest_files if file.endswith(week+'.csv')]
        week_indicator_files = [file for file in indicator_files if file.endswith(week+'.csv')]
        week_home_files = [file for file in home_loc_files if file.endswith(week+'.csv')]
        
        harvest_df = pd.DataFrame()

        for file in week_harvest_files:
            if '/._' not in file:
                print(file)
                temp_df = pd.read_csv(file)
                label = 0 if 'non_movers' in file else 1
                temp_df['harvest_label'] = label
                harvest_df = pd.concat([harvest_df, temp_df])

        # Get the list of all files in indicator_dir directory
            all_indicator_files = os.listdir(dir_individual_indicators)

        # Filter to get only .csv files for the specific week
        #for i in week_home_files:
        #    if '/._' not in file:
        #        home_location_df = pd.read_csv(i)[['customer_id', 'site_id']]
        #        home_location_df = home_location_df.drop_duplicates(['customer_id'])
        #        harvest_df = pd.merge(harvest_df, home_location_df, on='customer_id', how='outer')
        print('Step 1 done')

        
        for i in week_indicator_files:
            
            if '/._' not in i:
                print(i)
                # Special case for home_location files
    
                # Read the dataframe
                temp_df = pd.read_csv(i)
                temp_df = temp_df.drop_duplicates(['customer_id'])
                # Get the file name without extension and split at 'indicators'
                base_name = os.path.basename(i)
                name_without_ext = os.path.splitext(base_name)[0]
                name_parts = name_without_ext.split('_')
                week = [part for part in name_parts if part.isdigit() and part not in ['2020', '2021']][0]
                col_name_prefix_parts = [part for part in name_parts if part not in ['2020', '2021', week, 'indicators']]
                col_name_prefix = '_'.join(col_name_prefix_parts)

                new_columns = [col_name_prefix + "_" + col if col != 'customer_id' else col for col in temp_df.columns]

                # Assign the new column names to the dataframe
                temp_df.columns = new_columns

                # Merge the dataframe with the harvest dataframe
                harvest_df = pd.merge(harvest_df, temp_df, on='customer_id', how='outer')
        print('Step 2 done')
        # Append the week specific harvest_df to the weekly_df

        #weekly_df = pd.concat([weekly_df, harvest_df])
        #print(weekly_df.columns().tolist)
        
        # Read neighborhood indicators file
        #neighborhood_df = pd.read_csv(neighborhood_indicators_file)

        # Merge final_df with neighborhood_df
        # harvest_df = pd.merge(harvest_df, neighborhood_df, on='site_id', how='outer')
        # harvest_df = harvest_df[harvest_df['harvest_label'].isnull()==False].reset_index(drop=True)
        # Now, final_df contains all merged data

        harvest_df['week']=week

        final_df = pd.concat([final_df, harvest_df])

    final_df.to_csv("final.csv")


