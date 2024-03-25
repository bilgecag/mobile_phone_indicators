import pandas as pd
import numpy as np

def residential_inclusion(antenna_location, period, n1, n2):
    ##### Antenna dataset ####
    df = mean_antenna_usage(antenna_location, period, n1, n2)
    df = get_totals(df)
    group = pd.pivot_table(df, values=['total_migrant', 'total_locals'], index=['month', 'city', 'district'],
                           aggfunc=np.sum).reset_index()
    group['residential_inclusion']=group['total_migrant']/(group['total_migrant']+group['total_locals'])
    return group



########## Calculate the following #########

#n	the number of areas (census tracts) in the metropolitan area, ranked smallest to largest by land area

def get_number_of_tower(df, n1='2020-01-15 11', n2='2020-01-15 12'):
    # Between period n1 and n2 you will get the number of tower:
    if 'time' in df:
        t1 = datetime.strptime(n1, "%Y-%m-%d %H")
        t2 = datetime.strptime(n2, "%Y-%m-%d %H")
        count = len(df[(df['time']<n1)&(df['time']>n2)]['site_id'].unique())
        print("The number of tower between " + str(n1) + " " + str(n2) + " is")
    else:
        count = len(df['site_id'].unique())
        print("The number of tower is")
    return count

#m	the number of areas (census tracts) in the metropolitan area, ranked by increasing distance from the Central Business District (m = n)
def get_number_of_district(df, n1='2020-01-15 11', n2='2020-01-15 12'):
    # Between period n1 and n2 you will get the number of district:
    if 'time' in df:
        t1 = datetime.strptime(n1, "%Y-%m-%d %H")
        t2 = datetime.strptime(n2, "%Y-%m-%d %H")
        count = len(df[(df['time']<n1)&(df['time']>n2)]['district'].unique())
        print("The number of district between " + str(n1) + " " + str(n2) + " is")
    else:
        count = len(df['district'].unique())
        print("The number of district is")
    return count

def get_number_of_district(df, n1='2020-01-15 11', n2='2020-01-15 12'):
    # Between period n1 and n2 you will get the number of district:
    if 'time' in df:
        t1 = datetime.strptime(n1, "%Y-%m-%d %H")
        t2 = datetime.strptime(n2, "%Y-%m-%d %H")
        count = len(df[(df['time']<n1)&(df['time']>n2)]['city'].unique())
        print("The number of district between " + str(n1) + " " + str(n2) + " is")
    else:
        count = len(df['city'].unique())
        print("The number of district is")
    return city


def calculate_dissimilarity_isolation(df):
    df['total_all'] = df['refugee_population'] + df['local_population']
    df['total_local_ratio'] = df['total_locals'] / (df['local_population'] * df['representativeness_loc']).round(
        decimals=0)
    df['total_refugee_ratio'] = df['total_refugee'] / (df['refugee_population'] * df['representativeness_ref']).round(
        decimals=0)
    df['total_local_all_ratio'] = df['total_locals'] / (df['total_all'] * df['representativeness_loc']).round(
        decimals=0)
    df['total_refugee_all_ratio'] = df['total_refugee'] / (df['total_all'] * df['representativeness_ref']).round(
        decimals=0)

    ######### take cell as total

    df['total_all_cell'] = df['total_refugee_summed'] + df['total_locals_summed']
    df['total_local_ratio_cell'] = df['total_locals'] / df['total_locals_summed']
    df['total_refugee_ratio_cell'] = df['total_refugee'] / df['total_refugee_summed']
    df['total_local_all_ratio_cell'] = df['total_locals'] / df['total_all_cell']
    df['total_refugee_all_ratio_cell'] = df['total_refugee'] / df['total_all_cell']

    ####### indicators

    df['dissimilarity'] = abs(df['total_refugee_ratio'] - df['total_local_ratio']) * (1 / 2)

    df['dissimilarity_cell'] = abs(df['total_refugee_all_ratio_cell'] - df['total_local_ratio_cell']) * (1 / 2)

    df['isolation'] = (df['total_refugee_ratio']) * (df['total_local_all_ratio'])

    df['isolation_cell'] = (df['total_refugee_ratio_cell']) * (df['total_local_all_ratio_cell'])

    return df

def activity_space(r):
    # Wong et al., 2011: "Measuring segregation: an activity space approach"

    # In the original formulation of the exposure indices, the exposure of group a to group b (i.e., a x b) is

    df = read_fine_grained(r)
    df = df.merge(vor, on='site_id', how='left')
    df = df[df['voronoi_geometry'].isnull() == False].reset_index()
    df = get_totals(df, antenna=False)
    df['count'] = 1

    # Activity space–bounded exposure measures

    b_s_ijk = pd.pivot_table(data=df, index=ind, values=['count'], aggfunc=sum).reset_index()

    b_ij = pd.pivot_table(data=b_s_ijk, index=['site_id', 'day', 'month'], values=segs, aggfunc=sum).reset_index()
    for i in segs:
        b_ij = b_ij.rename({i: i + '_sum'}, axis=1)

    b_ij['total_sum'] = b_ij['migrant_sum'] + b_ij['local_sum']

    b_s_ijk = b_s_ijk.merge(b_ij, on=['site_id', 'day', 'month'], how='left')

    E_ijaxb = pd.pivot_table(data=b_s_ijk, index=['customer_id', 'day', 'month'], values=segs_all,
                             aggfunc=sum).reset_index()
    print(r, "is done")
    return E_ijaxb, b_ij

    # E_ijaxb['exposure_ij_local']=(E_ijaxb['migrant_sum']/E_ijaxb['total_sum'])*(1/E_ijaxb['local_sum'])
    # E_ijaxb['exposure_ij_migrant']=(E_ijaxb['local_sum']/E_ijaxb['total_sum'])*(1/E_ijaxb['migrant_sum'])
    # E_ijaxb=E_ijaxb.reset_index(drop=True)


def read_merge_activity_space_frame(file_name):
    # file_name='/Volumes/Extreme SSD/Data - Location/Hummingbird_Location_Datas/F_Fine_grained_mobility/fine_grained{}.txt'
    exposure_list = []
    site_id_list = []
    for i in range(6, 25):
        E_ijaxb, b_ij = activity_space(file_name.format(i))
    exposure_list.append(E_ijaxb)
    site_id_list.append(b_ij)
    df_exposure = pd.concat(exposure_list)
    df_site = pd.concat(site_id_list)
    return df_exposure, df_site

#xi	the minority population of all areas

#yi	the majority population of all areas

#ti	the total population of all areas

#X	the sum of all xi (the total minority population)

#Y	the sum of all yi (the total majority population)

#T	the sum of all ti (the total population)

#pi	the ratio of xi to ti (proportion of area i's population that is minority)

#P	the ratio of X to T (proportion of the metropolitan area's population that is minority)

#ai	the land area of area i

#A the sum of all ai (the total land area)

#n1	rank of area where the sum of all ti from area 1 (smallest in size) up to area n1 is equal to X

#T1	the sum of all ti in area 1 up to area n1

#n2	rank of area where the sum of all ti from area n (largest in size) down to area n2 is equal to X

#T2	the sum of all ti in area n2 up to area n

#dij	the distance between area i and area j centroids, where dii = (0.6ai)0.5

#cij	the exponential transform of -dij [= exp(-dij)]

#b	a shape parameter that determines how to weight the increments to segregation contributed by different portions of the Lorenz curve


######## Dissimilarity index ########

######## Isolation index ##########

######## Spatial integration Gini index ########

####### Exposure index ########

###### Local aspatial diversity index ######

