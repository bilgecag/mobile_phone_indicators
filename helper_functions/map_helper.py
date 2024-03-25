import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

#L = [(tt_coarse1_in, tt_coarse1_out),(tt_coarse2_in, tt_coarse2_out), (tt_coarse3_in, tt_coarse3_out),(tt_coarse4_in, tt_coarse4_out)\
#    ,(tt_coarse5_in, tt_coarse5_out),(tt_coarse6_in, tt_coarse6_out),(tt_coarse7_in, tt_coarse7_out),(tt_coarse8_in, tt_coarse8_out),\
#    (tt_coarse9_in, tt_coarse9_out),(tt_coarse10_in, tt_coarse10_out),(tt_coarse11_in, tt_coarse11_out),(tt_coarse12_in, tt_coarse12_out)]
#file_name = '/Volumes/Extreme SSD/Summary_Data/Coarse grained/sort approach/HL_sort_approach_{}.csv'
#df_list = []
#for i in range(1, 13):
#    df_list.append(pd.read_csv(file_name.format(i)))
#df = pd.concat(df_list)

#result = pd.read_csv('/Users/bilgecag/Desktop/total_migrant.csv')
#geojson = "/Users/bilgecag/Desktop/TURKCELL/Gorsellestirme/adm_json/tur_polbnda_adm1.shp"

#def getXY(pt):
#    return (pt.x, pt.y)

def read_map_city(file_directory):
    map_df = gpd.read_file(file_directory)
    # STEP 1 - Preprocessing:
    # MATCH THE NAMES
    map_df = map_df.rename(columns={'adm1_en':'city'})
    # TO FIND THE CENTROID OF THE POLYGONS:
    map_df["x"] = map_df.centroid.x
    map_df["y"] = map_df.centroid.y
    map_df['points'] = map_df.apply(
        lambda srs: Point(float(srs['x']), float(srs['y'])),
        axis='columns'
    )
    return map_df

def read_map_district(file_directory):
    map_df = gpd.read_file(file_directory)
    # STEP 1 - Preprocessing:
    # MATCH THE NAMES
    map_df = map_df.rename(columns={'adm1_en':'city'})
    map_df = map_df.rename(columns={'adm2_en':'district'})
    # TO FIND THE CENTROID OF THE POLYGONS:
    map_df["x"] = map_df.centroid.x
    map_df["y"] = map_df.centroid.y
    map_df['points'] = map_df.apply(
        lambda srs: Point(float(srs['x']), float(srs['y'])),
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

def read_map_district_tt(file_directory):
    gpd2 = gpd.read_file(geo)
    gpd2['center'] = gpd2['geometry'].centroid
    gpd2 = gpd2[['adm2_en','adm1_en','center']]
    gpd2 = gpd2.rename({'adm2_en': 'district', 'adm1_en': 'city'}, axis=1)
    df_il = pd.read_csv(il, sep=',', encoding="ISO-8859-1", error_bad_lines=False)
    df_ilce = pd.read_csv(ilce, sep=',', encoding="ISO-8859-1", error_bad_lines=False)
    df_ilce = df_ilce.rename({'BTS_DISTRICT': 'district'}, axis=1)
    df_il = df_il.rename({'CITY_DESC': 'city'}, axis=1)
    df_ilce['district'] = df_ilce['district'].replace({'SEHIRMERKEZI-YOZGAT':'YOZGAT','SEHIRMERKEZI-BITLIS':'BITLIS',\
    'SEHIRMERKEZI-USAK':'USAK','SEHIRMERKEZI-HAKKARI':'HAKKARI','SEHIRMERKEZI-KMARAS':'DULKADIROGLU',\
    'SEHIRMERKEZI-KIRIKKALE':'KIRIKKALE','SEHIRMERKEZI-ANTALYA':'KEPEZ','SEHIRMERKEZI-TEKIRDAG':'SULEYMANPASA',\
    'SEHIRMERKEZI-VAN':'EDREMIT','SEHIRMERKEZI-TUNCELI':'TUNCELI','SEHIRMERKEZI-BALIKESIR':'ALTIEYLUL','SEHIRMERKEZI-BATMAN':'BATMAN',\
    'SEHIRMERKEZI-MARDIN':'ARTUKLU','SEHIRMERKEZI-ADIYAMAN':'ADIYAMAN','SEHIRMERKEZI-KARABUK':'KARABUK','SEHIRMERKEZI-ISPARTA':'ISPARTA',\
    'SEHIRMERKEZI-AFYON':'AFYONKARAHISAR','SEHIRMERKEZI-AFYONKARAHISAR':'AFYONKARAHISAR', 'SEHIRMERKEZI-NEVSEHIR':'NEVSEHIR','SEHIRMERKEZI-BURSA':'BURSA',\
    'SEHIRMERKEZI-GAZIANTEP':'SAHINBEY','SEHIRMERKEZI-AGRI':'AGRI','SEHIRMERKEZI-KUTAHYA':'KUTAHYA','SEHIRMERKEZI-ELAZIG':'ELAZIG',\
    'SEHIRMERKEZI-EDIRNE':'EDIRNE','SEHIRMERKEZI-KARS':'KARS','SEHIRMERKEZI-ORDU':'ALTINORDU','SEHIRMERKEZI-MANISA':'MANISA','SEHIRMERKEZI-AFYON':'AFYONKARAHISAR',\
    'SEHIRMERKEZI-SIIRT':'SIIRT','SEHIRMERKEZI-TOKAT':'TOKAT','SEHIRMERKEZI-BINGOL':'BINGOL','SEHIRMERKEZI-MALATYA':'MALATYA',\
    'SEHIRMERKEZI-AMASYA':'AMASYA','SEHIRMERKEZI-MUGLA':'MUGLA','SEHIRMERKEZI-ARDAHAN':'ARDAHAN','SEHIRMERKEZI-BILECIK':'BILECIK',\
    'SEHIRMERKEZI-KIRKLARELI':'KIRKLARELI','SEHIRMERKEZI-ERZURUM':'ERZURUM','SEHIRMERKEZI-SANLIURFA':'SANLIURFA',\
    'SEHIRMERKEZI-CORUM':'CORUM','SEHIRMERKEZI-ZONGULDAK':'ZONGULDAK','SEHIRMERKEZI-ERZINCAN':'ERZINCAN',\
    'SEHIRMERKEZI-BARTIN':'BARTIN','SEHIRMERKEZI-DUZCE':'DUZCE','SEHIRMERKEZI-BURDUR':'BURDUR','SEHIRMERKEZI-KILIS':'KILIS',\
    'SEHIRMERKEZI-TRABZON':'TRABZON','SEHIRMERKEZI-RIZE':'RIZE','SEHIRMERKEZI-MUS':'MUS','SEHIRMERKEZI-ARTVIN':'ARTVIN',\
    'SEHIRMERKEZI-GUMUSHANE':'GUMUSHANE','SEHIRMERKEZI-DENIZLI':'DENIZLI','SEHIRMERKEZI-ESKISEHIR':'ESKISEHIR',\
    'SEHIRMERKEZI-CANAKKALE':'CANAKKALE','SEHIRMERKEZI-KASTAMONU':'KASTAMONU','SEHIRMERKEZI-GIRESUN':'GIRESUN',\
    'SEHIRMERKEZI-DIYARBAKIR':'BAGLAR','SEHIRMERKEZI-SAMSUN':'ATAKUM','SEHIRMERKEZI-SAKARYA':'ADAPAZARI',\
    'SEHIRMERKEZI-KOCAELI':'IZMIT','SEHIRMERKEZI-AYDIN':'EFELER','SEHIRMERKEZI-OSMANIYE':'OSMANIYE',\
    'SEHIRMERKEZI-BAYBURT':'BAYBURT','SEHIRMERKEZI-NIGDE':'NIGDE','SEHIRMERKEZI-SIRNAK':'SIRNAK','SEHIRMERKEZI-IGDIR':'IGDIR',\
    'SEHIRMERKEZI-YALOVA':'YALOVA','SEHIRMERKEZI-SINOP':'SINOP','SEHIRMERKEZI-KARAMAN':'KARAMAN','SEHIRMERKEZI-KIRSEHIR':'KIRSEHIR',\
    'SEHIRMERKEZI-AKSARAY':'AKSARAY','SEHIRMERKEZI-CANKIRI':'CANKIRI','SEHIRMERKEZI-MERSIN':'MEZITLI','SEHIRMERKEZI-SIVAS':'SIVAS','SEHIRMERKEZI-BOLU':'BOLU',\
    'SULTANGAZÝ':'SULTANGAZI','ZEYTÝNBURNU':'ZEYTINBURNU','BEÞÝKTAÞ':'BESIKTAS',\
    'CEYLANPINARI':'CEYLANPINAR','BÜYÜKÇEKMECE':'BUYUKCEKMECE','GAZÝOSMANPAÞA':'GAZIOSMANPASA','BEYOÐLU':'BEYOGLU',\
    'DÝKÝLÝ':'DIKILI','ONDOKUZMAYIS': '19 MAYIS','ÇATALCA': 'CATALCA','EDREMIT ': 'EDREMIT','GÜNGÖREN':'GUNGOREN',\
    'BAÐCILAR':'BAGCILAR','ARNAVUTKÖY':'ARNAVUTKOY','AYDINLAR':'AYDINLAR','DULKADIROÐLU':'DULKADIROGLU','BAYRAMPAÞA':'BAYRAMPASA',\
    'ÞÝÞLÝ':'SISLI','EYUP':'EYUPSULTAN','KAÐITHANE':'KAGITHANE','ISKILP':'ISKILIP','BÝGA':'BIGA','KÜÇÜKÇEKMECE':'KUCUKCEKMECE',\
    'BAÞAKÞEHÝR':'BASAKSEHIR','BAHÇELÝEVLER':'BAHCELIEVLER','EYÜP':'EYUPSULTAN','FATÝH':'FATIH','KAZAN':'KAHRAMANKAZAN'})

    df_ilce['district'] = df_ilce['district'].replace({'BURSA':'OSMANGAZI','MANISA':'YUNUSEMRE','ERZURUM':'AZIZIYE',\
    'MALATYA':'YESILYURT','MUGLA':'MENTESE','SANLIURFA':'EYYUBIYE','TRABZON':'ORTAHISAR','ESKISEHIR':'ODUNPAZARI','DENIZLI':'MERKEZEFENDI'})

    df_ilce['district'] = df_ilce['district'].replace({'ALTINYAYLA-SIVAS':'SIVAS', 'KALE-DENIZLI':'MERKEZEFENDI', 'YENIPAZAR-BILECIK':'BILECIK',\
    'AYVACIK-SAMSUN':'ATAKUM', 'KALE-MALATYA':'MENTESE', 'BAYAT-CORUM':'CORUM', 'GOLBASI-ANKARA':'CANKAYA','EDREMIT-VAN':'IPEKYOLU', 'DUZICI-OSMANIYE':'OSMANIYE',\
    'PINARBASI-KAYSERI':'HACILAR','ORTAKOY-CORUM':'CORUM', 'ULUBEY-USAK':'USAK', 'BOZKURT-KASTAMONU':'KASTAMONU','OVACIK-TUNCELI':'TUNCELI', 'GONEN-ISPARTA':'ISPARTA',\
    'YENICE-KARABUK':'KARABUK', 'PAZAR-TOKAT':'TOKAT','AYDINLAR':'ADIYAMAN', 'KEMER-BURDUR':'BURDUR', 'YESILYURT-TOKAT':'TOKAT', 'KOPRUBASI-TRABZON':'ORTAHISAR',\
    'ORHANÝYE_TT':'IZMIT', 'BAHCE-OSMANIYE':'OSMANIYE','AKSU-ISPARTA':'ISPARTA', 'YENISEHIR-DIYARBAKIR':'BAGLAR', 'SARAY-VAN':'IPEKYOLU','EREGLI-ZONGULDAK':'ZONGULDAK',\
                                                   'KADIRLI-OSMANIYE':'OSMANIYE'})
    df_il['city'] = df_il['city'].replace({'AFYON':'AFYONKARAHISAR'})

    gpd2 = gpd2.merge(df_il, on= ['city'], how = 'inner')
    gpd2 = gpd2.merge(df_ilce, on= ['district'], how = 'inner')
    gpd2['city-district']=gpd2['city'].astype(str)+'-'+gpd2['district'].astype(str)
    #gpd = gpd[gpd['center'].isnull()==False]
    gpd2['lng'] = gpd2.center.apply(lambda p: p.x)
    gpd2['lat'] = gpd2.center.apply(lambda p: p.y)
    gpd2['lat'] = gpd2['lat'].round(6)
    gpd2['lng'] = gpd2['lng'].round(6)
    return gpd2

def merge_dgmm_data(df,offical):
    dg = pd.read_csv(offical, delimiter = ';', usecols = ['city','refugee_population','local_population', 'month'])


    compare = pd.pivot_table(df, values= ['total_refugee','total_locals'], index=['month',\
                            'city'],
                           aggfunc=np.sum).reset_index()
    compare = compare.merge(dg, on=['month','city'], how='left')
    compare['representativeness_ref']=compare['total_refugee']/compare['refugee_population']
    compare['representativeness_loc']=compare['total_locals']/compare['local_population']
    compare = compare[['representativeness_loc','representativeness_ref','month','city','refugee_population','local_population']]
    df = df.merge(compare, on=['month','city'], how='left')
    return df








