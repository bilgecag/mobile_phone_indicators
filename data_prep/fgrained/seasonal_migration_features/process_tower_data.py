import pandas as pd
import numpy as np
import os

from shapely.geometry import Point

import geopandas as gpd

def read_tower_data(tower_location):

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
def read_map_district(file_directory):
    map_df = gpd.read_file(file_directory)
    # STEP 1 - Preprocessing:
    # MATCH THE NAMES
    map_df = map_df.rename(columns={'adm1_en':'city'})
    map_df = map_df.rename(columns={'adm2_en':'district'})
    # TO FIND THE CENTROID OF THE POLYGONS:
    map_df["lat"] = map_df.centroid.x
    map_df["lng"] = map_df.centroid.y
    map_df['coordinates'] = map_df.apply(
        lambda srs: Point(float(srs['lat']), float(srs['lng'])),
        axis='columns'
    )
    map_df = map_df.drop(['OBJECTID', 'adm1_tr', 'adm1', 'pcode', 'Shape_Leng', 'Shape_Area', 'adm0_en', 'adm0_tr', 'adm_0'], axis = 1)

    # Bazi ilçe isimleri aynı olduğu için yeni bir unique ID yaratıyoruz

    map_df['city_district'] = map_df['city'].astype(str) + '_' + map_df['district'].astype(str)

    map_df['city_district'] = map_df['city_district'].replace({'ADIYAMAN_ADIYAMAN': 'ADIYAMAN_ADIYAMAN MERKEZ', 'AFYONKARAHISAR_AFYONKARAHISAR': 'AFYONKARAHISAR_AFYONKARAHISAR MERKEZ',
                               'AGRI_AGRI': 'AGRI_AGRI MERKEZ', 'AKSARAY_AKSARAY': 'AKSARAY_AKSARAY MERKEZ', 'AMASYA_AMASYA': 'AMASYA_AMASYA MERKEZ',
                               'ANKARA_KAHRAMANKAZAN': 'ANKARA_KAZAN', 'ARDAHAN_ARDAHAN': 'ARDAHAN_ARDAHAN MERKEZ', 'ARTVIN_ARTVIN': 'ARTVIN_ARTVIN MERKEZ',
                               'BARTIN_BARTIN': 'BARTIN_BARTIN MERKEZ', 'BATMAN_BATMAN': 'BATMAN_BATMAN MERKEZ', 'BAYBURT_BAYBURT': 'BAYBURT_BAYBURT MERKEZ',
                               'BILECIK_BILECIK': 'BILECIK_BILECIK MERKEZ', 'BINGOL_BINGOL': 'BINGOL_BINGOL MERKEZ', 'BITLIS_BITLIS': 'BITLIS_BITLIS MERKEZ',
                               'BOLU_BOLU': 'BOLU_BOLU MERKEZ', 'BURDUR_BURDUR': 'BURDUR_BURDUR MERKEZ', 'CANAKKALE_CANAKKALE': 'CANAKKALE_CANAKKALE MERKEZ',
                               'CANKIRI_CANKIRI': 'CANKIRI_CANKIRI MERKEZ', 'CORUM_CORUM': 'CORUM_CORUM MERKEZ',                            'DUZCE_DUZCE': 'DUZCE_DUZCE MERKEZ', 'EDIRNE_EDIRNE': 'EDIRNE_EDIRNE MERKEZ', 'ELAZIG_ELAZIG': 'ELAZIG_ELAZIG MERKEZ',
                               'ERZINCAN_ERZINCAN': 'ERZINCAN_ERZINCAN MERKEZ', 'GIRESUN_GIRESUN': 'GIRESUN_GIRESUN MERKEZ', 'GUMUSHANE_GUMUSHANE': 'GUMUSHANE_GUMUSHANE MERKEZ',
                               'HAKKARI_HAKKARI': 'HAKKARI_HAKKARI MERKEZ', 'IGDIR_IGDIR': 'IGDIR_IGDIR MERKEZ', 'ISPARTA_ISPARTA': 'ISPARTA_ISPARTA MERKEZ',
                               'ISTANBUL_EYUPSULTAN': 'ISTANBUL_EYUP', 'KARABUK_KARABUK': 'KARABUK_KARABUK MERKEZ', 'KARAMAN_KARAMAN': 'KARAMAN_KARAMAN MERKEZ',
                               'KARS_KARS': 'KARS_KARS MERKEZ', 'KASTAMONU_KASTAMONU': 'KASTAMONU_KASTAMONU MERKEZ', 'KILIS_KILIS': 'KILIS_KILIS MERKEZ',
                               'KIRIKKALE_KIRIKKALE': 'KIRIKKALE_KIRIKKALE MERKEZ', 'KIRKLARELI_KIRKLARELI': 'KIRKLARELI_KIRKLARELI MERKEZ', 'KIRSEHIR_KIRSEHIR': 'KIRSEHIR_KIRSEHIR MERKEZ',
                               'KUTAHYA_KUTAHYA': 'KUTAHYA_KUTAHYA MERKEZ', 'MUS_MUS': 'MUS_MUS MERKEZ', 'NEVSEHIR_NEVSEHIR': 'NEVSEHIR_NEVSEHIR MERKEZ',
                               'NIGDE_NIGDE': 'NIGDE_NIGDE MERKEZ', 'OSMANIYE_OSMANIYE': 'OSMANIYE_OSMANIYE MERKEZ',
                               'RIZE_RIZE': 'RIZE_RIZE MERKEZ', 'SAMSUN_19 MAYIS': 'SAMSUN_ONDOKUZMAYIS',
                               'SIIRT_TILLO': 'SIIRT_AYDINLAR', 'SIIRT_SIIRT': 'SIIRT_SIIRT MERKEZ', 'SINOP_SINOP': 'SINOP_SINOP MERKEZ',
                               'SIRNAK_SIRNAK': 'SIRNAK_SIRNAK MERKEZ', 'SIVAS_SIVAS': 'SIVAS_SIVAS MERKEZ', 'TOKAT_TOKAT': 'TOKAT_TOKAT MERKEZ',
                               'TUNCELI_TUNCELI': 'TUNCELI_TUNCELI MERKEZ', 'USAK_USAK': 'USAK_USAK MERKEZ', 'YALOVA_YALOVA': 'YALOVA_YALOVA MERKEZ',
                               'YOZGAT_YOZGAT': 'YOZGAT_YOZGAT MERKEZ',
                               'ZONGULDAK_ZONGULDAK': 'ZONGULDAK_ZONGULDAK MERKEZ',
                              })

    return map_df

tower_location = r"/Volumes/Extreme SSD/Data - Location/Hummingbird_Location_Data/A_Cell_Tower_Locations/cell_city_district.txt"
tur='/Volumes/Extreme SSD/Geodirectory/turkey_administrativelevels0_1_2/tur_polbnda_adm2.shp'
loc_vor= "/Volumes/Extreme SSD/MPD_based_indicators_of_migration/Seasonal_migration/voronoi.geojson"
df_tower= read_tower_data(tower_location)
df_tower['city_district']=df_tower['city'].astype(str)+'_'+df_tower['district'].astype(str)
gdf_tur=read_map_district(tur)
gdf_tur=gdf_tur[['city_district','lat','lng']]

vor=gpd.read_file(loc_vor)
vor=vor.to_crs(epsg=4326)  
vor['coordinates']=vor['geometry'].centroid
vor["lat"] = vor.centroid.x
vor["lng"] = vor.centroid.y
vor=vor[['lat','lng','site_id']]

df_tower=df_tower.merge(vor,on='site_id',how='left')

merged_df = pd.merge(df_tower, gdf_tur, on='city_district', how='left')

merged_df['lat_x'] = merged_df['lat_x'].fillna(merged_df['lat_y'])
merged_df['lng_x'] = merged_df['lng_x'].fillna(merged_df['lng_y'])

merged_df = merged_df.drop(['lat_y', 'lng_y'], axis=1)

merged_df = merged_df.rename({'lat_x':'lat','lng_x':'long'},axis=1)

merged_df.to_csv('/Volumes/Extreme SSD/MPD_based_indicators_of_migration/Seasonal_migration/towers.csv')

