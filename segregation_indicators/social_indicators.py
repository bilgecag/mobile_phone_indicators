import pandas as pd
from helper_functions.tower_helper import read_tower_data
from helper_functions.utilities import *
from helper_functions.antenna_helper import antenna_aggregate_calls


def social_connectedness_antenna(df, tower_location, data_direction, spatial_aggregation_level,
                                 temporal_aggregation_level, which_segment):

    df_processed = df.copy()
    validate_dataframe_columns(df_processed, data_type='aggregated')
    loc_level = get_location_level(spatial_aggregation_level)
    suffix, opposite_suffix = get_suffixes_based_on_direction(data_direction)
    if which_segment:
        try:
            df_processed = df_processed[df_processed['segment_caller_callee'].isin(which_segment)].reset_index(drop=True)
        except:
            raise ValueError(
                f"Introduce all the segments that will be filtered in brackets. E.g. which_segment=['1-1', '2-2', '1-2', '2-1']")

    df_totals = antenna_aggregate_calls(df_processed, tower_location, data_direction,
                                        spatial_aggregation_type='per_location',
                                        spatial_aggregation_level=spatial_aggregation_level,
                                        temporal_aggregation_level=temporal_aggregation_level, segment=True,
                                        exclude_same_location_calls=True)
    df_totals = df_totals.rename(columns={'call_count': 'total_call_count', 'call_duration': 'total_call_duration'})
    df_processed = antenna_aggregate_calls(df_processed, tower_location, data_direction,
                                           spatial_aggregation_type='location_to_location',
                                           spatial_aggregation_level=spatial_aggregation_level,
                                           temporal_aggregation_level=temporal_aggregation_level, segment=True,
                                           exclude_same_location_calls=True)
    df_totals = df_totals[['total_call_count', 'total_call_duration', f"{loc_level}_{suffix}", 'segment_caller_callee',
                           temporal_aggregation_level]]
    df_processed = df_processed.merge(df_totals,
                                      on=[f"{loc_level}_{suffix}", 'segment_caller_callee', temporal_aggregation_level], \
                                      how='left'). \
        rename(columns={loc_level: f"{loc_level}_{suffix}", 'total_call_count': f"total_call_count_{suffix}", \
                        'total_call_duration': f"total_call_duration_{suffix}"})  # .drop(columns=['site_id'])
    df_processed = df_processed.merge(df_totals, right_on=[f"{loc_level}_{suffix}", 'segment_caller_callee',
                                                           temporal_aggregation_level], \
                                      left_on=[f"{loc_level}_{opposite_suffix}", 'segment_caller_callee',
                                               temporal_aggregation_level], \
                                      how='left'). \
        rename(columns={loc_level: f"{loc_level}_{suffix}", 'total_call_count': f"total_call_count_{opposite_suffix}", \
                        'total_call_duration': f"total_call_duration_{opposite_suffix}",
                        f"{loc_level}_{suffix}_x": f"{loc_level}_{suffix}"
                        }).drop(columns=[f"{loc_level}_{suffix}_y"])

    # Filtering based on segment_caller_callee requirements

    df_processed['sci'] = df_processed['call_duration'] / (
                df_processed[f"total_call_duration_{opposite_suffix}"] * df_processed[f"total_call_duration_{suffix}"])
    df_processed['normalized_sci'] = (df_processed['sci'] - df_processed['sci'].min()) / (
                df_processed['sci'].max() - df_processed['sci'].min())

    # Calculate combined SCI considering nulls
    df_processed['combined_sci'] = df_processed[['sci']].apply(lambda x: x.dropna().mean(), axis=1)

    return df_processed.reset_index(drop=True)

def ei_index(df, tower_location, data_direction, spatial_aggregation_level,
             temporal_aggregation_level, which_segment, majority_segment='1', minority_segment='2'):
    df_processed = df.copy()
    validate_dataframe_columns(df_processed, data_type='aggregated')
    loc_level = get_location_level(spatial_aggregation_level)
    suffix, opposite_suffix = get_suffixes_based_on_direction(data_direction)

    if which_segment:
        df_processed = df_processed[df_processed['segment_caller_callee'].isin(which_segment)].reset_index(drop=True)

    df_totals = antenna_aggregate_calls(df_processed, tower_location, data_direction,
                                        spatial_aggregation_type='per_location',
                                        spatial_aggregation_level=spatial_aggregation_level,
                                        temporal_aggregation_level=temporal_aggregation_level, segment=True,
                                        exclude_same_location_calls=True)

    # Rename columns for clarity
    df_totals = df_totals.rename(columns={'call_count': 'total_call_count',
                                          'call_duration': 'total_call_duration'})

    # Assuming majority to minority = '1-2' and minority to majority = '2-1'
    # Assuming minority to minority = '2-2' and majority to majority = '1-1'
    # Adjust segment names based on your actual data

    # Group by spatial and temporal levels
    grouped = df_totals.groupby([f"{loc_level}_{suffix}", temporal_aggregation_level])

    # Calculate EI Index within each group

    ei_scores = grouped.apply(lambda x: calculate_ei(x, majority_segment, minority_segment)).reset_index(name='EI_index')

    return ei_scores

def calculate_ei(group, majority_segment, minority_segment):
    # Assume segments are defined correctly
    maj_to_min = f"{majority_segment}-{minority_segment}"
    min_to_maj = f"{minority_segment}-{majority_segment}"
    min_to_min = f"{minority_segment}-{minority_segment}"
    maj_to_maj = f"{majority_segment}-{majority_segment}"

    total_calls = group['total_call_count'].sum()
    # Filter within group based on 'segment_caller_callee' and aggregate
    maj_to_min_calls = group.loc[group['segment_caller_callee'] == maj_to_min, 'total_call_count'].sum()
    min_to_maj_calls = group.loc[group['segment_caller_callee'] == min_to_maj, 'total_call_count'].sum()
    min_to_min_calls = group.loc[group['segment_caller_callee'] == min_to_min, 'total_call_count'].sum()
    maj_to_maj_calls = group.loc[group['segment_caller_callee'] == maj_to_maj, 'total_call_count'].sum()

    # Calculate the EI index
    ei_index = float(maj_to_min_calls + min_to_maj_calls - min_to_min_calls - maj_to_maj_calls) / total_calls if total_calls else 0
    return ei_index

def contact_diversity(df):
    df_processed = df.copy()
    validate_dataframe_columns(df_processed, data_type='individual')
    # Assuming correct segment information is aligned with 'customer_id'
    diversity = df_processed.groupby('customer_id').agg({'segment_caller': pd.Series.nunique})
    diversity.rename(columns={'segment_caller': 'segment_diversity'}, inplace=True)
    return diversity

def calculate_segment_ratios(df):
    df_processed = df.copy()
    validate_dataframe_columns(df_processed, data_type='individual')
    # Fill NaN values in 'segment_caller' and 'segment_callee' with 0 (representing unknown segment)
    df_processed['segment_caller'].fillna(0, inplace=True)
    df_processed['segment_callee'].fillna(0, inplace=True)

    # Create a new column to store the relevant segment based on call type
    df_processed['relevant_segment'] = np.where(df_processed['call_type'] == 1, df_processed['segment_caller'],
                                                df_processed['segment_callee'])

    # Count the number of calls per relevant segment for each customer
    segment_counts = df_processed.groupby(['customer_id', 'relevant_segment'])['call_type'].count().unstack(
        fill_value=0)

    # Calculate the total number of calls per customer
    total_calls = segment_counts.sum(axis=1)

    # Calculate the ratio of calls for each segment per customer
    segment_ratios = segment_counts.div(total_calls, axis=0)

    # Rename columns to 'call_ratio_seg_X'
    segment_ratios.columns = ['call_ratio_seg_' + str(int(col)) for col in segment_ratios.columns]

    return segment_ratios.reset_index()

def calculate_city_relations_counts(df):
    df_processed = df.copy()
    validate_dataframe_columns(df_processed, data_type='individual')

    unique_customer_ids = df_processed['customer_id'].unique()
    chunk_size = len(unique_customer_ids) // 4  # Aim for 4 chunks; adjust based on your data size/preferences

    # Placeholder for results from each chunk
    results = []

    for start_idx in range(0, len(unique_customer_ids), chunk_size):
        end_idx = start_idx + chunk_size
        customer_ids_chunk = unique_customer_ids[start_idx:end_idx]

        # Filter the DataFrame for the current chunk of customer IDs
        df_chunk = df_processed[df_processed['customer_id'].isin(customer_ids_chunk)]

        # Process each chunk using vectorized operations
        df_chunk.loc[:, 'customer_city'] = np.where(df_chunk['call_type'] == 1, df_chunk['city_id_callee'],
                                                    df_chunk['city_id_caller'])
        df_chunk.loc[:, 'other_party_city'] = np.where(df_chunk['call_type'] == 1, df_chunk['city_id_caller'],
                                                       df_chunk['city_id_callee'])

        #df_chunk['customer_city'] = np.where(df_chunk['call_type'] == 1, df_chunk['city_id_callee'], df_chunk['city_id_caller'])
        #df_chunk['other_party_city'] = np.where(df_chunk['call_type'] == 1, df_chunk['city_id_caller'], df_chunk['city_id_callee'])
        df_chunk = df_chunk.dropna(subset=['customer_city', 'other_party_city'], how='all')

        # Calculate unique counts for the chunk
        customer_city_count = df_chunk.groupby('customer_id')['customer_city'].nunique()
        other_party_city_count = df_chunk.groupby('customer_id')['other_party_city'].nunique()

        # Combine the counts into a single DataFrame for the chunk
        city_counts_chunk = pd.concat([customer_city_count, other_party_city_count], axis=1).reset_index()
        city_counts_chunk.columns = ['customer_id', 'cities_been_to_count', 'cities_in_communication_count']

        # Append chunk results to the results list
        results.append(city_counts_chunk)

    # Concatenate all chunk results into a single DataFrame
    city_counts_final = pd.concat(results).reset_index(drop=True)

    return city_counts_final
