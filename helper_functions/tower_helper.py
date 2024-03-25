import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
from shapely.geometry import Point
import numpy as np
import seaborn as sns

def read_tower_data(tower_location,voronoi_file=None,crs="EPSG:4326"):
    if crs:
        target_crs=crs
    else:
        target_crs = "EPSG:4326"

    if not voronoi_file:
        voronoi_file="/Volumes/Extreme Pro/Cell_Tower_Locations/turkcell_voronoi/voronoi.shp"

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

    tower_sorted_district = tower.sort_values(by=['city', 'district'])
    tower_sorted_district = tower_sorted_district.drop_duplicates(subset=['city', 'district'])

    tower_sorted_district['city_district_id'] = tower_sorted_district.groupby(['city', 'district']).ngroup() + 1

    tower_sorted_city = tower.sort_values(by=['city'])
    tower_sorted_city = tower_sorted_city.drop_duplicates(subset=['city'])
    tower_sorted_city['city_id'] = tower_sorted_city.groupby(['city']).ngroup() + 1


    tower_merged = tower.merge(tower_sorted_district[['city', 'district', 'city_district_id']], on=['city', 'district'])
    tower_merged = tower_merged.merge(tower_sorted_city[['city', 'city_id']], on=['city'])
    tower_merged["city_district"] = tower_merged['city'].astype(str) + '_' + tower_merged['district'].astype(str)

    voronoi_gdf=gpd.read_file(voronoi_file, geometry='geometry').rename(columns={"matcher":"site_id"})[["site_id","geometry"]]
    voronoi_gdf.crs = "EPSG:5636"
    voronoi_gdf = voronoi_gdf.to_crs(target_crs)

    tower_merged=tower_merged.merge(voronoi_gdf,on="site_id",how="right")
    tower_merged=gpd.GeoDataFrame(tower_merged,geometry="geometry")
    tower_merged['centroid'] = tower_merged['geometry'].centroid
    tower_merged['lat'] = tower_merged['centroid'].y
    tower_merged['lng'] = tower_merged['centroid'].x
    tower_merged=tower_merged[tower_merged['geometry'].isnull()==False].reset_index(drop=True)
    tower_merged=tower_merged.rename(columns={"geometry": "voronoi_geometry","centroid":"geometry"})
    #print('There are {} cell towers in the datas   et'.format(df.site_id.nunique()))
    return tower_merged


def tower_data_to_dict(tower_location):
    
    # This function reads the tower location, and creates the dictionaries out of it.
    # These dictionaries will allow quick look-up of attribute values given a site_id.
    # If a tower location is provided, read the tower data from this CSV file.
    # If no tower location is provided, use a fallback method to load the tower data

    df_tower = pd.read_csv(tower_location, sep=';')

    site_to_city = dict(zip(df_tower.site_id, df_tower.city)) # maps site_id to city
    site_to_district = dict(zip(df_tower.site_id, df_tower.district)) # maps site_id to district
    site_to_long = dict(zip(df_tower.site_id, df_tower.long)) # maps site_id to longitude
    site_to_lat = dict(zip(df_tower.site_id, df_tower.lat)) # maps site_id to latitude

    # Return all four dictionaries
    return site_to_city, site_to_district, site_to_long, site_to_lat


#def detect_decoding_errors_line(l, _s=_surrogates.finditer):
#   """Return decoding errors in a line of text
#
#    Works with text lines decoded with the surrogateescape
#    error handler.
#
#    Returns a list of (pos, byte) tuples
#    """
#    # DC80 - DCFF encode bad bytes 80-FF
#    return [(m.start(), bytes([ord(m.group()) - 0xDC00]))
#            for m in _s(l)]

def get_cartesian(lat=None,lon=None):
    lat, lon = np.deg2rad(lat), np.deg2rad(lon)
    R = 6371 # radius of the earth
    x = R * np.cos(lat) * np.cos(lon)
    y = R * np.cos(lat) * np.sin(lon)
    z = R *np.sin(lat)
    return x,y, #z


def find_sites_in_buffer(gdf, circle):
    intersection = gpd.overlay(gdf, circle, how='intersection')
    sites = intersection['matcher'].unique().tolist()
    return sites


def plot_sites(sites, df, time_variable="DATE", event_date=None):
    # Convert string to Timestamp

    df = df[df['CALER_MATCHER'].isin(sites)].reset_index(drop=True)
    # Set up the plot
    fig, ax = plt.subplots(figsize=(84.7, 30.27))

    sns.set(style="whitegrid")

    # Calculate sum of CALLER_CALLE_SEGMENT_T per date and plot
    ax = sns.lineplot(x=time_variable, y="CALLER_CALLE_SEGMENT_T", data=df.groupby(time_variable).sum())

    if event_date is not None:
        event_timestamp = pd.Timestamp(event_date)
        # Add a vertical line for the event date
        plt.axvline(x=event_timestamp, color='red', linestyle='--', linewidth=2)

    # Setting the font size for x-ticks and y-ticks
    ax.tick_params(axis='both', labelsize=80)

    # Optionally, set the font size for labels and title
    ax.set_xlabel("Date", fontsize=80)
    ax.set_ylabel("Call Segment Total", fontsize=80)
    ax.set_title("Call Patterns Over Time", fontsize=80)
    fig.autofmt_xdate()

    # Show the plot
    plt.show()

def create_point_buffer(lat, lon, radius, input_crs, target_crs):
    point = gpd.GeoSeries([Point(lon, lat)], crs=input_crs)
    circle = gpd.GeoDataFrame(point,geometry=0, crs=input_crs)
    circle = circle.to_crs(target_crs)
    circle[0] = circle[0].buffer(radius)
    return circle #gpd.GeoDataFrame({'geometry': buffer}, crs=target_crs)