from shapely.ops import cascaded_union
from geovoronoi import voronoi_regions_from_coords, points_to_coords, coords_to_points
import shapely
import pandas as pd
import geopandas as gpd


def intersection_df(grid_cells, poly_shapes_list):
    list_intersect = []
    for i, g1 in enumerate(grid_cells):
        for j, g8 in enumerate(poly_shapes_list):
            if g1.intersects(g8):
                L = (g1.intersection(g8).area/g1.area)*100
                _list = [L,i,j]
                list_intersect.append(_list)
    df=pd.DataFrame(list_intersect, columns = ['Area','Grid_No','region_ID'])
    return df

def from_voronoi_to_grid(df_grid_loc, poly_shapes_list):
    df_grid_cells = gpd.read_file(df_grid_loc)
    grid_cells=df_grid_cells.geometry.to_list()
    df = grid_cells.intersects(poly_shapes_list)
    #df = intersection_df(grid_cells, poly_shapes_list)
    df_grid_cells['Grid_No']=range(0,len(df_grid_cells))
    df = df.merge(gdf, on = 'region_ID', how ='left')
    df =df.merge(df_grid_cells, on = 'Grid_No', how ='left')
    df=df[['voronoi_geometry','id','id_2','geometry_y', 'Area', 'Grid_No', 'region_ID', 'BTS_ID']]
    return df

def from_towers_to_voronoi(df_tower, region_map,crs="EPSG:4326"):
    if crs:
        target_crs=crs
    else:
        target_crs = "EPSG:4326"

    #df_sites_list = df_tower.site_id.unique().tolist()
    gdf = gpd.read_file(df_tower)
    #gdf['site_id'] = gdf['site_id'].astype(int)
    # if gdf['voronoi_geometry'].iloc[1]==str:
    #gdf['voronoi_geometry'] = gdf['voronoi_geometry'].apply(lambda x: shapely.wkt.loads(x))
    #gdf = gpd.GeoDataFrame(gdf, crs="EPSG:5636", geometry='voronoi_geometry')
    #gdf['pts'] = gdf.centroid
    #gdf = gdf.set_geometry('pts')
    #gdf = gdf[gdf['site_id'].isin(df_sites_list) == True]

    # Solve the issue with non-existent voronois
    #gdf = gdf.to_crs(epsg=5636)

    boundary = gpd.read_file(region_map)
    boundary = boundary.to_crs(target_crs)
    #boundary = boundary[(boundary['adm1_en'] == 'ISTANBUL')]# | (boundary['adm1_en'] == 'KOCAELI')]
    #boundary = boundary.to_crs(epsg=5636)  # 4326
    #boundary_istanbul = gpd.read_file(city_map)
    #boundary_istanbul = boundary_istanbul.to_crs(epsg=5636)  ###ADDED LATER
    #boundary_istanbul = boundary_istanbul[
    #    (boundary_istanbul['adm1_en'] == 'ISTANBUL') ]#| (boundary_istanbul['adm1_en'] == 'KOCAELI')]

    gdf = gpd.sjoin(gdf, boundary, how='inner', op='within').reset_index()
    gdf_proj = gdf.to_crs(boundary.crs)
    boundary_shape = cascaded_union(boundary.geometry)
    coords = points_to_coords(gdf_proj.geometry)

    gdf['pts_id'] = range(len(coords))
    print(coords)
    poly_shapes, pts = voronoi_regions_from_coords(coords, boundary_shape)
    poly_shapes_vals = poly_shapes.values()
    poly_shapes_list = list(poly_shapes_vals)
    voronoi = pd.DataFrame(poly_shapes.items(), columns=['region_ID', 'voronoi_geometry'])
    voronoi2 = pd.DataFrame(pts.items(), columns=['region_ID', 'pts_id'])
    voronoi = voronoi.merge(voronoi2, how='left', on='region_ID')
    voronoi['pts_id'] = voronoi['pts_id'].str.get(0)
    gdf = gdf.merge(voronoi, how='left', on='pts_id')

    return gdf, poly_shapes_list



