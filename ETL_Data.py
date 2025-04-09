# -*- coding: utf-8 -*-
"""
Created on Wed Apr  9 10:28:26 2025

@author: aston
"""
import pandas as pd
import glob
import os
import datetime
import numpy as np
import geopandas as gp
now1 = datetime.now()
current_time = now1.strftime("%H:%M:%S")
current_date = now1.strftime("%Y/%m/%d")
today = pd.to_datetime('now').strftime("%d.%m.%y")
current_path = os.getcwd()

##% ETL DATA
#%%
#%%
#%% CHECK DATA DUPLICATED
import glob
from collections import namedtuple
import pandas as pd

lst_file = []
for file in glob.glob(current_path + '*.xlsx'):
    lst_file.append(file)

lst_df = []
for i in range(1, len(lst_file) + 1):
    lst_df.append('df' + str(i))

df = namedtuple('Cdfs',
                lst_df
                )(*[pd.read_excel(file) for file in lst_file])
lst_df = []
for i in df:
    lst_df.append(i)
df = pd.concat(lst_df, axis=0)   


check = df['Project_name'].value_counts().reset_index()
check = check.loc[check['Project_name']>=2]
lst_p = check['index'].tolist()

df = df.drop_duplicates(subset = ['Project_name'])

df_sum = pd.read_excel(current_path + '/data/MAP_Layer_Apartment_ALLinVN.xlsx')
df_sum.columns


df_sum = df_sum[['Project_name', 'Province', 'District', 'Ward']].drop_duplicates(subset = ['Project_name'])

test = df.merge(df_sum, how = 'left', on = ['Project_name'])
test = test.loc[test['Province'].isnull()]



test.to_excel(current_path + '/data/BDS_Fill_Province.xlsx', index = False)

# SYNCE DATA WITH VIETNAM GEO SYSTEM
df = pd.read_excel(current_path + str(today) + '.xlsx')

                  

df = pd.read_excel(current_path +'/data/BDS_Fill_Province.xlsx')


df['Status'] = df['Status'].astype(str)                  
df['Status'] = df['Status'].str.replace("[", "")
df['Status'] = df['Status'].str.replace("]", "")                  
df['Status'] = df['Status'].str.replace("'", "")       

df.columns

df.drop(['Province','District', 'Ward'], axis = 1, inplace = True)
#%%
df_old = pd.read_excel('''D:/MOHO - DOANH SỐ/FILE GỐC HARAVAN/TESTING/CREATE Longtitude, Latitude/MAP_Layer_Apartment_ALL.xlsx''')
df_old['Province'] = 'Hồ Chí Minh'
check = df_old['Source'].value_counts()
df_old_1 = df_old.loc[df_old['Source'] == 'batdongsan.com.vn']
print(list(df_old_1.columns))
df_old_1 = df_old_1[['Project_name', 'Province','District']]


df = df.merge(df_old_1, how = 'left', on = ['Project_name'])

df_null = df.loc[df['Province'].isnull()]
df.info()


df_null = df_null[['Project_location']].reset_index()
df_seperated = pd.DataFrame(df_null.Project_location.str.split(', ', 3).tolist(),columns=['COL_1', 'COL_2', 'COL_3', 'COL_4']) 


df_seperated['COL_4'].fillna(df_seperated['COL_3'], inplace= True) 

def fix_COL3(r):
    if r['COL_4'] == r['COL_3']:
        return r['COL_2']
    else:
        return r['COL_3']

df_seperated['COL_3'] = df_seperated.apply(fix_COL3, axis = 1)


df_all = df.copy()

df = df_null.join(df_seperated).drop(['index'], axis = 1)

mapping = gp.GeoDataFrame.from_file('D:/GEO - MAP/MAP_VN_2021/gadm36_VNM_namth1.shp')  
mapping_copy = mapping.copy()
#%%
df['address'] = df['Project_location'].str.lower().str.strip()
df['province'] = df['COL_4'].str.lower().str.strip()
df['district'] = df['COL_3'].str.lower().str.strip()
df['ward'] = df['COL_2'].str.lower().str.strip()

mapping['NAME_1'] = mapping['NAME_1'].str.lower().str.strip()
mapping['NAME_2'] = mapping['NAME_2'].str.lower().str.strip()
mapping['NAME_3'] = mapping['NAME_3'].str.lower().str.strip()

mapping.rename(columns={'NAME_1':'province',
                        'NAME_2':'district',
                        'NAME_3':'ward'}, inplace=True)

a_mapping = mapping[['province', 'district', 'ward']]


check__a_mapping = a_mapping['district'].value_counts()

#%%
# df = df[['province', 'district', 'ward']]

check = df['province'].value_counts()

# lst_null = set(a_mapping['province'].tolist()) - set(df['province'].tolist())

df['province'] = df['province'].astype(str)


def fix_province(r):
    temp = unidecode.unidecode(r['province'])
    if 'hcm' in temp:
        return 'ho chi minh'
    elif 'ha noi' in temp:
        return 'ha noi'
    elif 'binh duong' in temp:
        return 'binh duong'
    elif 'khanh hoa' in temp:
        return 'khanh hoa'
    
    elif 'quy nhon' in temp:
        return 'binh dinh'  
    elif 'da lat' in temp:
        return 'lam dong'
    elif 'vung tau' in temp:
        return 'ba ria - vung tau'
    
    else:
        return temp

df['province'] = df.apply(fix_province, axis = 1) 
df['province'] = df['province'].replace('nan', np.nan)


#%% lower and remove accents in df & mapping  # LOẠI bỏ dấu trong mapping
def remove_accents(r):
    if type(r) is str:
        return unidecode.unidecode(r)

for col in ['province','district','ward']:
    mapping[col] = mapping[col].apply(remove_accents)
for col in ['province','district','ward']:
    df[col] = df[col].apply(remove_accents)

check = df.loc[df['province'] == 'ho chi minh']['district'].value_counts()

#%% turn mapping to dict of dict of list
mapping = {k:v.groupby('district')['ward'].apply(list).to_dict()
            for k,v in mapping.groupby('province')}
cols = list(df.columns)


check = df['province'].value_counts()

df['province'] = df['province'].astype(str)

def modify_district(r):
    remove_list = ['thanh pho','quan','huyen','thi xa']
    result = r['district']
    if type(r['district']) is str:
        for i in remove_list:
            result = result.replace(i,'').strip()
    return result
df['district'] = df.apply(modify_district, axis=1)


#%%
df['district'] = df['district'].astype(str)

def fix_district(r):
    temp = r['district']
    if len(temp) <= 3 & len(temp) >= 1:
        return temp.replace('d', '')
    else:
        return temp
df['district'] = df.apply(fix_district, axis = 1)             


def fix_district2(r):
    temp_0 = r['province']
    # temp = r['district_key']
    temp_1 = r['district']
    # if temp  == None:
    # if 'bac tan uyen' in temp_1:
    #     return 'tan uyen'
    # elif 'nam tu liem' in temp_1 or 'bac tu liem' in temp_1:
    #     return 'tu liem'
    if 'duong minh chau' in temp_1:
        return 'duong minh chau'
    elif 'bau bang' in temp_1:
        return 'ben cat district'
    elif 'buon me thuot' in temp_1:
        return 'buon ma thuot'
    elif 'g trach' in temp_1:
        return 'quang trach'
    elif 'phan rang' in temp_1:
        return 'phan rang-thap cha'
    elif 'thu duc' in temp_1 or  temp_1 == '9' or temp_1 == '2':
        return 'thu duc'
    else:
        return temp_1
df['district'] = df.apply(fix_district2, axis = 1)



#%%

def modify_ward(r):
    remove_list = ['phuong 0','phuong','xa','thi tran']
    result = r['ward']
    if type(r['ward']) is str: 
        for i in remove_list:
            result = result.replace(i,'').strip()
    return result
df['ward'] = df.apply(modify_ward, axis=1)
    


#%% add province_key, district_key, address_key

def province_key(r):
    if type(r['province']) is str:
        for k in mapping.keys():
            if r['province'] == k.replace('tp. ',''):
                return k
                break
df['province_key'] = df.apply(province_key, axis=1)

def district_key(r):
    if type(r['province_key']) is str and type(r['district']) is str:
        for k,v in mapping.items():
            if r['province_key'] == k:
                for k2 in v:
                    k2_temp = k2
                    remove_list = ['city','district','distri','town']
                    for i in remove_list:
                        k2_temp = k2_temp.replace(i,'').strip()
                    if r['district'] in k2_temp:
                        return k2
                        break
df['district_key'] = df.apply(district_key, axis=1)

df['ward'] = df['ward'].astype(str)
def ward_key(r):
    if type(r['province_key']) is str and type(r['district_key']) is str and \
        type(r['ward'] is str):
        for k,v in mapping.items():
            if r['province_key'] == k:
                for k2,v2 in v.items():
                    if r['district_key'] == k2:
                        for i in v2:
                            if r['ward'] in i.replace('ward','').strip():
                                return i
                                break
df['ward_key'] = df.apply(ward_key, axis=1)  

check__province = df[['province_key', 'province']].drop_duplicates()
check__district = df[['district_key', 'district', 'province']].drop_duplicates()
check__ward = df[['ward_key', 'ward']].drop_duplicates()


check = df.loc[df['province'] == 'ho chi minh']['district'].value_counts()

check_province_key = df.loc[df['province_key'].isnull()].drop_duplicates()
check_district_key = df.loc[df['district_key'].isnull()].drop_duplicates()
check_ward_key = df.loc[df['ward_key'].isnull()].drop_duplicates()

check_ward_key = check_ward_key.sort_values(by = ['province_key', 'district_key'], ascending = [True, True])

check_ward_key = check_ward_key[['Project_location', 'COL_1', 'COL_2', 'COL_3','province_key', 'district_key', 'ward_key']]




check_ward_key.to_excel(current_path + 'data/Fill_na_Province_Apartment.xlsx', index = False) # FIX DATA BY HAND THIS FILE



#%% fill na PROVINCE, DISTRICT, WARD
fill_na = pd.read_excel(
    current_path + 'data/Fill_na_Province_Apartment.xlsx')
print(list(fill_na.columns))

fill_na = fill_na[['Project_location', 'province_key', 'district_key', 'ward_key']]
fill_na = fill_na.drop_duplicates(subset = 'Project_location')


#%%

fill_na['Project_location'] = fill_na['Project_location'].astype(str)

fill_na.set_index(keys='Project_location', inplace=True)
fill_na = fill_na.T.to_dict(orient='list')

def fill_na_province(r):
    if r['province_key'] is None:
        for k,v in fill_na.items():
            if r['Project_location'] == k:
                return v[0]
                break
    else:
        return r['province_key']
df['province_key'] = df.apply(fill_na_province, axis=1)

def fill_na_district(r):
    if r['district_key'] is None:
        for k,v in fill_na.items():
            if r['Project_location'] == k:
                return v[1]
                break
    else:
        return r['district_key']
df['district_key'] = df.apply(fill_na_district, axis=1)

def fill_na_ward(r):
    if r['ward_key'] is None:
        for k,v in fill_na.items():
            if r['Project_location'] == k:
                return v[2]
                break
    else:
        return r['ward_key']
df['ward_key'] = df.apply(fill_na_ward, axis=1)


check_null_address = df.loc[(df['province_key'].isnull()) | (df['district_key'].isnull()) | (df['ward_key'].isnull())]

if check_null_address.shape[0] >= 1:
    print('                        ')
    print('                        ')
    print('     ADDRESS IS NULL PROVINCE/ DISTRICT/ WARD: {}'.format(check_null_address))


# MORE DETAIL PROJECT NAME:
check_null2 = check_null_address.merge(df_all[['Project_name', 'Project_location']], how = 'left', on = ['Project_location'])


#%%
df.info()
df_all.rename({'Province': 'province_key', 'District': 'district_key'}, axis = 1, inplace = True)
df = df[['Project_location', 'province_key', 'district_key', 'ward_key']]

df_null_o = df.copy()

# df.rename({'province_key': 'province', 'district_key': 'district', 'ward_key': 'ward'}, axis = 1, inplace = True)



for col in list(df.columns):
    df[col] = df[col].replace('None', np.nan)
    df[col] = df[col].replace('nan', np.nan)

    
df_map = pd.read_excel(current_path + 'data/CREATE Longtitude, Latitude/Fix_Province.xlsx')

df_province = df_map[['province', 'province_fix']].drop_duplicates()
df_district = df_map[['province', 'district', 'district_fix']].drop_duplicates() # district duplicated
df_ward = df_map[['province', 'district', 'ward', 'ward_fix']].drop_duplicates() # ward duplicated


#%%
df = df.merge(df_province, how = 'left', left_on = 'province_key', right_on = 'province')#ok  #left_on = 'province_key', right_on = 'province'


df = df.merge(df_district, how = 'left', left_on = ['province_key', 'district_key'], 
                right_on = ['province', 'district']) #ok

df = df.merge(df_ward, how = 'left', left_on = ['province_key', 'district_key', 'ward_key'], 
                right_on = ['province', 'district', 'ward'])

#%% fix outlier:
def fix_ward(r):
    temp = r['Project_location']
    if temp == 'Số 199 Hồ Tùng Mậu, phường Cầu Diễn, Bắc Từ Liêm, Hà Nội':
        return 'Cầu Diễn'
    elif temp == 'Đường D5, Phường Khánh Bình, Tân Uyên, Bình Dương':
        return 'Khánh Bình'
    elif temp == 'Hải Châu, Đà Nẵng':
        return 'Hải Châu I'
    elif temp == 'A2.1-5.1 Nguyễn Hữu Thọ, quận Hải Châu, Đà Nẵng':
        return 'Hải Châu I'
    else:
        return r['ward_fix']
df['ward_fix'] = df.apply(fix_ward, axis = 1)

df = df[['Project_location', 'province_fix', 'district_fix', 'ward_fix']]

df.rename({'province_fix': 'province_key', 'district_fix': 'district_key', 'ward_fix': 'ward_key'}, axis = 1, inplace = True)

df_1 = df_all.loc[df_all['province_key'].isnull()]
df_2 = df_all.loc[df_all['province_key'].notnull()]


df_1.drop(['province_key', 'district_key'], axis = 1, inplace = True)

df = df.drop_duplicates(subset = 'Project_location')

df_1 = df_1.merge(df, how = 'left', on = 'Project_location')
df_complete = pd.concat([df_1, df_2])
#%%
df_old = pd.read_excel(current_path + '/data/MAP_Layer_Apartment_ALL.xlsx')
df_old['Province'] = 'Hồ Chí Minh'
check = df_old['Source'].value_counts()
df_old_1 = df_old.loc[df_old['Source'] == 'hotrothutuc.com']
df_old_1['Status'] = 'Đã bàn giao'


print(list(df_old_1.columns))


df_complete['Source'] = 'batdongsan.com.vn'

print(list(df_complete.columns))


df_complete.rename({'Summary': 'project_description', 'province_key': 'Province', 'district_key': 'District', 'ward_key': 'Ward'}, axis = 1, inplace = True)
df_complete['Map_layer'] = 'Apartment'

check_set = set(list(df_complete.columns)) - set(list(df_old_1.columns))


df_old_1 = df_old_1[['Project_name', 'Project_location', 'project_description', 'Link_project', 'Source', 
                     'Map_layer', 'District', 'Ward', 'Province', 'Status']]


df = pd.concat([df_complete, df_old_1])


#%% MERGE 
df_address = pd.read_excel(current_path + '/data/Apartment_Project_with_full_corr.xlsx')

df = df.merge(df_address, how = 'left', on = 'Project_location')

df_address_null = df.loc[df['Latitude'].isnull()][['Project_location']]

print('Số lượng địa chỉ dự án Apartment null: {}'.format(df_address_null))

# df.to_excel('D:/MOHO - DOANH SỐ/FILE GỐC HARAVAN/TESTING/CREATE Longtitude, Latitude/MAP_Layer_Apartment_ALLinVN.xlsx', sheet_name = 'Apartment_Project',index = False)

#UPLOAD Address on GGS:
import unidecode
import gspread 
from gspread_dataframe import set_with_dataframe

gc = gspread.service_account(filename= current_path + '/data/project-aftersale-moho-8ae2fb5f1b6b.json')

sh = gc.open_by_key('1C4itMuXyK3_b0D4s-0wXaQuDxSox4GVcz4lCw8cGx8M')
worksheet = sh.get_worksheet(0)
worksheet.clear()

# APPEND DATA TO SHEET
set_with_dataframe(worksheet, df_address_null)  # Run Geocoding first


#%% GET DATA AFTER GEOCODING
df_address_null = worksheet.get_all_values()
df_address_null = pd.DataFrame(df_address_null, columns = ['Project_location', 'Latitude', 'Longitude'])  #'Unnamed: 0' 

df_address_null = df_address_null.iloc[1: , :]

df_address_null.to_excel(current_path +'/data/Apartment_Project_Temporary.xlsx', index = False)
df_address_null = pd.read_excel(current_path + '/data/Apartment_Project_Temporary.xlsx')


df_address = pd.read_excel(current_path + '/data//Apartment_Project_with_full_corr.xlsx')

df_address = pd.concat([df_address, df_address_null]).drop_duplicates()



df_address = df_address.loc[df_address['Latitude'].notnull()]


df_address.to_excel(current_path +'/data/Apartment_Project_with_full_corr.xlsx', index = False) 


#%% ETL INFO PROJECT:
import ast
df = pd.read_excel(current_path +'/data/MAP_Layer_Apartment_ALLinVN.xlsx', sheet_name = 'Apartment_Project')

df["Info"] = df["Info"].astype('O') # chuyển đổi cột chứa list thành type Object phòng khi cột Info là type str
def fix_info(r):
    temp = r['Info']
    if temp == '[]':
        return np.nan
    else:
        return temp
df.Info = df.apply(fix_info, axis = 1)

df1 = df.loc[df['Info'].notnull()]
df2 = df.loc[~(df['Info'].notnull())]

df1.loc[:,'Info'] = df1.loc[:,'Info'].apply(lambda x: ast.literal_eval(x))

df1 = df1.reset_index().drop(['index'], axis = 1)

# 
df3 = pd.DataFrame([pd.Series(x) for x in df1.Info]) 
df3.columns = ['Info_{}'.format(x+1) for x in df3.columns]

df1 = df1.join(df3)

lst = ['Info_1', 'Info_2', 'Info_3', 'Info_4']
for i in lst:
    df1[i] = df1[i].astype(str)

lst = ['Info_1', 'Info_2', 'Info_3', 'Info_4']
for i in lst:
    df1[i] = df1[i].str.replace(',', '')
def add_price(r):
    lst = [r['Info_1'], r['Info_2'], r['Info_3'], r['Info_4']]
    for i in lst:
        if 'triệu/m²' in i:
            return i
        continue
df1['Apartment_Price'] = df1.apply(add_price, axis = 1)
    
def add_project_acreage(r):
    lst = [r['Info_1'], r['Info_2'], r['Info_3'], r['Info_4']]
    for i in lst:
        if 'triệu/m²' not in i:
            if 'ha' in i or 'm²' in i:
                return i
            continue
df1['Project_acreage'] = df1.apply(add_project_acreage, axis = 1)

def add_Apartment_Quantity(r):
    lst = [r['Info_1'], r['Info_2'], r['Info_3'], r['Info_4']]
    for i in lst:
        try:
            if float(i) >= 100:
                return i
            continue
        except:
            None
df1['Apartment Quantity'] = df1.apply(add_Apartment_Quantity, axis = 1)

def add_building_qty(r):
    lst = [r['Info_1'], r['Info_2'], r['Info_3'], r['Info_4']]
    for i in lst:
        try:
            if float(i) < 100:
                return i
            continue
        except:
            None
df1['Building Quantity'] = df1.apply(add_building_qty, axis = 1)

df1.drop(['Info_1', 'Info_2', 'Info_3', 'Info_4'], axis = 1, inplace = True)

df = pd.concat([df1, df2])


df['Ward_Group'] = df['District'] + ' -' + df['Ward'] 

df.to_csv(current_path + '/data/MAP_Layer_Apartment_ALLinVN.csv',index = False)
