# clusterer = tower_clusterer(ds, 'admin2', 'ID_2')
import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Polygon, LineString
from sklearn.metrics import DistanceMetric
from scipy.spatial.distance import squareform
from scipy.cluster.hierarchy import linkage, fcluster
from helper_functions.tower_helper import read_tower_data
from helper_functions.map_helper import read_map_district
import warnings
warnings.filterwarnings("ignore", category=UserWarning, message="Geometry is in a geographic CRS.")


def cluster_towers(tower_df, dist_metric='haversine', t=1):
    """
    Clusters towers based on their geographical location.
    """
    dist = DistanceMetric.get_metric(dist_metric)
    # Convert to radians for distance calculation
    radians = tower_df.copy()
    radians[['lat_radians', 'lng_radians']] = np.radians(tower_df[['lat', 'lng']])

    # Cluster using the haversine formula and Ward's method
    clusters = fcluster(
        linkage(squareform(dist.pairwise(radians[['lng_radians', 'lat_radians']].to_numpy()) * 6373), method='ward'),
        t=t, criterion='distance')
    radians['cluster'] = clusters

    # Drop radian columns to avoid confusion
    radians.drop(columns=['lat_radians', 'lng_radians'], inplace=True)

    return radians


def get_centroids(sites_with_clusters):
    """
    Computes centroids of clusters, including handling single-point clusters.
    """
    centroids = []
    for cluster_num in sites_with_clusters['cluster'].unique():
        subset = sites_with_clusters[sites_with_clusters['cluster'] == cluster_num]
        if len(subset) == 1:
            # Single-point cluster; centroid is the point itself
            centroid = subset.iloc[0][['lng', 'lat']]
        elif len(subset) == 2:
            # Two-point cluster; use the midpoint
            line = LineString(subset[['lng', 'lat']].values)
            centroid = [line.centroid.x, line.centroid.y]
        else:
            # Multi-point cluster; use the convex hull centroid
            polygon = Polygon(subset[['lng', 'lat']].values).convex_hull
            centroid = [polygon.centroid.x, polygon.centroid.y]

        centroids.append({'cluster': cluster_num, 'centroid_LNG': centroid[0], 'centroid_LAT': centroid[1]})

    # Merge the centroids back into the original DataFrame
    centroids_df = pd.DataFrame(centroids)
    sites_with_clusters = pd.merge(sites_with_clusters, centroids_df, on='cluster', how='left')

    return sites_with_clusters


def map_to_regions(sites_with_clusters, shape_gpd, region_var):
    """
    Maps clusters to regions based on spatial join.
    """
    # Create GeoDataFrame from clusters for spatial join
    sites_gpd = gpd.GeoDataFrame(sites_with_clusters, geometry=gpd.points_from_xy(sites_with_clusters['centroid_LNG'],
                                                                                  sites_with_clusters['centroid_LAT']),
                                 crs="EPSG:4326")

    # Spatial join with regions
    joined = gpd.sjoin(sites_gpd, shape_gpd, how="left", op="intersects")
    joined = joined.rename(columns={region_var: 'region'})

    # Drop the geometry column to avoid confusion and select relevant columns
    joined.drop(columns='geometry', inplace=True)

    print(joined.head())
    return joined[['site_id', 'lat', 'lng', 'cluster', 'region']]

