
############# Utility functions used throughout
import os

def get_location_level(spatial_level):
    levels = {
        'district': 'city_district_id',
        'city': 'city_id',
        'site': 'site_id',
    }
    if spatial_level not in levels:
        raise ValueError("Invalid value for spatial_level. It should be 'site', 'district', or 'city'.")
    return levels[spatial_level]

def apply_temporal_aggregation(df, temporal_aggregation_level):
    """
    Applies temporal aggregation to the DataFrame based on the specified level.

    Parameters:
    - df (pd.DataFrame): The DataFrame to which the temporal aggregation will be applied.
    - temporal_aggregation_level (str): The level of temporal aggregation, 'day', 'week', or 'month'.

    Returns:
    - pd.DataFrame: The DataFrame with the new temporal aggregation column added.
    """
    if 'week' in temporal_aggregation_level:
        df['week'] = df['time'].dt.isocalendar().week
    elif 'day' in temporal_aggregation_level:
        df['day'] = df['time'].dt.date
    elif 'month' in temporal_aggregation_level:
        df['month'] = df['time'].dt.month
    else:
        raise ValueError("Invalid value for temporal_aggregation_level. It should be 'day', 'week', or 'month'.")

    return df


def get_suffixes_based_on_direction(data_direction):
    """
    Determines the suffixes for caller and callee based on call direction.

    Parameters:
    - data_direction (str): The direction of the data, either 'OUTGOING' or 'INCOMING'.

    Returns:
    - tuple: A tuple containing the suffix for the direct participant and the opposite participant.
    """
    if data_direction == 'OUTGOING':
        suffix = 'caller'
        opposite_suffix = 'callee'
    elif data_direction == 'INCOMING':
        suffix = 'callee'
        opposite_suffix = 'caller'
    else:
        raise ValueError("Invalid value for data_direction. It should be 'OUTGOING' or 'INCOMING'.")

    return suffix, opposite_suffix


def validate_dataframe_columns(df, data_type='individual'):
    """
    Validates the presence of required columns in a DataFrame based on the specified data type.

    Parameters:
    - df (pd.DataFrame): The DataFrame to validate.
    - data_type (str): The type of data, either 'individual' or 'aggregated', to specify the expected columns.

    Raises:
    - ValueError: If the DataFrame does not contain the required columns for the specified data type.
                  For 'individual', it must have customer_id/caller_id/callee_id/user_id.
                  For 'aggregated', it must have site_id/caller_site_id/callee_site_id.
                  If neither individual nor aggregated columns are present, a recognition error is raised.
    """
    # Define the column groups for validation
    customer_id_variants = ['customer_id', 'caller_id', 'callee_id', 'user_id']
    site_id_variants = ['site_id', 'caller_site_id', 'callee_site_id']

    # Determine presence of customer and site columns
    customer_columns_present = any(col in df.columns for col in customer_id_variants)
    site_columns_present = any(col in df.columns for col in site_id_variants)

    # Apply validation logic based on data type
    if data_type == 'individual':
        if not customer_columns_present and not site_columns_present:
            raise ValueError("The DataFrame does not contain recognized data columns for individual-level data.")
        elif not customer_columns_present and site_columns_present:
            raise ValueError(
                "The DataFrame is missing customer-related columns but contains site columns, indicating a mismatch for individual-level data.")
    elif data_type == 'aggregated':
        if not site_columns_present and not customer_columns_present:
            raise ValueError("The DataFrame does not contain recognized data columns for aggregated data.")
        elif not site_columns_present and customer_columns_present:
            raise ValueError(
                "The DataFrame is missing site-related columns but contains customer columns, indicating a mismatch for aggregated data.")
    else:
        raise ValueError("Invalid data_type specified. Choose 'individual' or 'aggregated'.")


def ostype_add_directory(os_type, directory):
    base_dir_mac = "/Volumes"
    base_dir_linux = "/media/f140926"
    base_dir = base_dir_mac if os_type.lower() == 'mac' else base_dir_linux
    based_directory = base_dir + directory
    return based_directory

def matrix_of_possibilities(tl, idl, x, y):
    tl_idl_tuple = [(i, j) for i in idl for j in tl]
    y = pd.DataFrame(tl_idl_tuple, columns=[x, y])
    return y

def save_and_load_parquet(df, filename, ds):
    # write parquet
    df.write.mode('overwrite').parquet(filename)
    #load parquet
    df = ds.spark.read.format("parquet").load(filename)
    return df

def save_csv(matrix, path, filename):
    # write to csv
    matrix.repartition(1).write.mode('overwrite').format('com.databricks.spark.csv') \
        .save(os.path.join(path, filename), header = 'true')
    # move one folder up and rename to human-legible .csv name
    if databricks:
        dbutils.fs.mv(dbutils.fs.ls(path + '/' + filename)[-1].path,
                  path + '/' + filename + '.csv')
        # remove the old folder
        dbutils.fs.rm(path + '/' + filename + '/', recurse = True)

    else:
        os.rename(glob.glob(os.path.join(path, filename + '/*.csv'))[0],
                  os.path.join(path, filename + '.csv'))
        shutil.rmtree(os.path.join(path, filename))


############  # Plotting

def zero_to_nan(values):
    """Replace every 0 with 'nan' and return a copy."""
    values[ values==0 ] = np.nan
    return values

def fill_zero_dates(pd_df):
    pd_df = pd_df[~pd_df.index.isnull()].sort_index()
    msisdnx = pd.date_range(pd_df.index[0], pd_df.index[-1])
    pd_df = pd_df.reindex(msisdnx, fill_value= 0)
    return pd_df
