import pandas as pd

def matrix_of_possibilities(tl, idl, x, y):
    tl_idl_tuple = [(i, j) for i in idl for j in tl]
    y = pd.DataFrame(tl_idl_tuple, columns=[x, y])
    return y

def read_coarse_tt(file_directory_in, file_directory_out):
    coarse = pd.read_csv(file_directory_in, sep=',', encoding="ISO-8859-1", error_bad_lines=False)
    coarse2 = pd.read_csv(file_directory_out, sep=',', encoding="ISO-8859-1", error_bad_lines=False)
    coarse = coarse.append(coarse2)
    coarse = coarse.rename({'OUTGOING_SITE_ID': 'site_id', 'TIMESTAMP': 'time', 'NUMBER_OF_CALLS': 'total_local',
                      'NUMBER_OF_REFUGEE_CALLS': 'total_refugee', }, axis=1)
    coarse['time'] =  pd.to_datetime(coarse['time'], format='%d-%m-%Y %H:%M')
    coarse['month'] = coarse['time'].dt.month
    return coarse

def _monthly_location_individual(df):
    M =[]
    for i in df.CALLER_ID.unique():
        district = df[df['CALLER_ID']==i].groupby(['district']).count().sort_values(by='time', ascending=False).iloc[0].name
        #length = len(df[df['CALLER_ID']==i])
        month = str(df.month.unique())
        M.append((district,i, month))
    home = pd.DataFrame(M, columns=['district', 'CALLER_ID', 'month'])
    return home

def read_coarse_grained(coarse_grained_location):
    df = pd.read_csv(coarse_grained_location,
                        sep="|", skiprows = 0,
                        header=0, encoding ='ISO-8859-1', error_bad_lines=False)
    df = df.drop(['Unnamed: 0', 'Unnamed: 6'], axis = 1)
    df = df.rename(columns=lambda x: x.strip())
    for i in df.columns:
        df[i] = df[i].astype(str)
    df = df.apply(lambda x: x.str.strip())
    df = df.iloc[1: , :]

    for i in df.columns[1:3]:
        df[i] = df[i].astype(int)


    df['time'] =  pd.to_datetime(df['time'], infer_datetime_format=True)
    df = df.rename(columns={'city_id': 'city','id':'district'})
    df['city_district']=df['city'].astype(str)+'_'+df['district'].astype(str)
    df['month'] = df['time'].dt.month


    return df

