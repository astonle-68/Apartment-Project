# -*- coding: utf-8 -*-
"""
Created on Fri Aug  4 11:27:10 2023

@author: aston
"""
from IPython import get_ipython
get_ipython().magic('reset -sf') 
import pandas as pd
import numpy as np
import glob
import geopandas as gp
import unidecode
from collections import namedtuple
from gspread_dataframe import set_with_dataframe
from datetime import datetime, timedelta
from time import sleep
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from selenium.webdriver.chrome.options import Options 
from datetime import datetime, timedelta

now1 = datetime.now()
current_time = now1.strftime("%H:%M:%S")
current_date = now1.strftime("%Y/%m/%d")
today = pd.to_datetime('now').strftime("%d.%m.%y")
options = Options()


# git remote add origin https://github.com/astonle-68/Apartment-Project.git
#%%
lst_p = []
for i in range(0, 75):  # 74, 160
    options.binary_location = "D:/MOHO - DOANH SỐ/Google/Chrome Beta/Application/chrome.exe" # https://stackoverflow.com/questions/45500606/set-chrome-browser-binary-through-chromedriver-in-python
    driver = webdriver.Chrome(chrome_options=options, executable_path="D:/Chromedriver/BETA/chromedriver.exe") # https://googlechromelabs.github.io/chrome-for-testing/
    driver.set_window_size(960, 540) # 1920, 1080
    
    
    
    Status = []
    n_p = 1488 # số dự án 
    
    # Dự án HCM:https://duan.batdongsan.com.vn/can-ho-chung-cu-tp-hcm/p
    # Tất cả dự án: https://batdongsan.com.vn/du-an-can-ho-chung-cu/p
    c = 0
    c_1 = 0
    set_speed = 0.5
    
        
    Product_name = []
    driver.get('https://batdongsan.com.vn/du-an-can-ho-chung-cu/p'+ str(i)) # https://duan.batdongsan.com.vn/can-ho-chung-cu/p
    sleep(set_speed)
    
    SCROLL_PAUSE_TIME = set_speed
    last_height = driver.execute_script("return document.body.scrollHeight")
    
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        sleep(SCROLL_PAUSE_TIME)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
    
    driver.implicitly_wait(set_speed)   # time to load data from website - default: 10
    items = driver.find_elements_by_xpath('//div[@class="js__project-card js__card-project-web re__prj-card-full"]') #items = driver.find_elements_by_xpath('//*[@id="main"]/div/div[3]/div/div[2]/div/div[2]/div')
    for item in items:
        Name = Name = item.find_element_by_xpath('.//h3[@class="re__prj-card-title"]').text  # ok
        print('                      ')
        print('                      ')
        print('Project: {}'.format(Name))
        Project_location = Project_location = item.find_element_by_xpath('.//div[@class="re__prj-card-location"]').text #ok
        
        print('Location: {}'.format(Project_location))
        # Project_price = Project_price = item.find_element_by_xpath('.//span[@class="re__prj-card-config-value"]').text
        # info.append(Project_price)
        Project_status = []
        for table in item.find_elements_by_xpath('.//div[@class="re__project-open re__prj-tag-info"]'): 
            status = [item.text for item in table.find_elements_by_xpath(".//*[self::label]")]
            Project_status.append(status)

        for table in item.find_elements_by_xpath('.//div[@class="re__project-na re__prj-tag-info"]') : 
            status = [item.text for item in table.find_elements_by_xpath(".//*[self::label]")]
            Project_status.append(status)
        for table in item.find_elements_by_xpath('.//div[@class="re__project-prepare re__prj-tag-info"]') : 
            status = [item.text for item in table.find_elements_by_xpath(".//*[self::label]")]
            Project_status.append(status)
        for table in item.find_elements_by_xpath('.//div[@class="re__project-finish re__prj-tag-info"]') : 
            status = [item.text for item in table.find_elements_by_xpath(".//*[self::label]")]

            Project_status.append(status)
        print('Status: {}'.format(status))
        des = []
        for infos in item.find_elements_by_xpath('.//div[@class="re__prj-card-info-content"]'):
            info = [item.text for item in infos.find_elements_by_xpath('.//span[@class="re__prj-card-config-value"]')]
            des.append(info)
            print('Info Project: {}'.format(des))
        summary = summary = item.find_element_by_xpath('.//div[@class="re__prj-card-summary"]').text
        link = link = item.find_element_by_xpath('.//a').get_attribute('href')
     
        dict_p = {'Project_name': Name, 'Status': Project_status, 'Info': des, 'Summary': summary,'Project_location': Project_location, 'Link_project': link}
        lst_p.append(dict_p)
        c+=1
        c_1+= float(1*100/n_p) 
        # print('Product completed: {}, ({:.2f}%)'.format(c, c_1))
        
    driver.quit()  

df = pd.DataFrame(lst_p)           
df['Source'] = 'batdongsan.com.vn'        
df['Map_layer'] = 'Apartment'
    
df.rename({"Summary": 'project_description'}, axis = 1, inplace = True)
    
    
df.to_excel('''D:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Data/Project_Estate_BDS/Raw_Data/BDS_Data_''' + str(today) + '.xlsx', index= False) 

            
import pyttsx3

engine = pyttsx3.init()
voices = engine.getProperty('voices')
engine.setProperty('voice', voices[1].id)
# rate = engine.getProperty('rate')
# engine.setProperty('rate', rate-80)
engine.say(', CRAWLING ESTATE PROJECT, COMPLETED'.format(i))
engine.runAndWait()               
            
#%%
#%%
#%% CHECK DATA
import glob
from collections import namedtuple
import pandas as pd

lst_file = []
for file in glob.glob('''/*.xlsx'''):
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

df_sum = pd.read_excel('''D:/MOHO - DOANH SỐ/FILE GỐC HARAVAN/TESTING/Data For Tableau Dashboard - Sales By Demographics & GEO/MAP_Layer_Apartment_ALLinVN.xlsx''')
df_sum.columns


df_sum = df_sum[['Project_name', 'Province', 'District', 'Ward']].drop_duplicates(subset = ['Project_name'])

test = df.merge(df_sum, how = 'left', on = ['Project_name'])
test = test.loc[test['Province'].isnull()]



test.to_excel('''D:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Data/Project_Estate_BDS/Data_Need_ETL/BDS_Fill_Province.xlsx''', index = False)
#%%
#%%
#%% ETL DATA 
# df = pd.read_excel('''D:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Data/Project_Estate_BDS/Raw_Data/BDS_Data_''' + str(today) + '.xlsx')

df = pd.read_excel('''D:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Data/Project_Estate_BDS/Raw_Data/BDS_Data_26.03.25.xlsx''')
                   

# df = pd.read_excel('''D:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Data/Project_Estate_BDS/Raw_Data/BDS_Data_26.02.24.xlsx''')

df = pd.read_excel('''D:/MOHO's OTHER PROJECT/Crawling DATA BY PYTHON/Data/Project_Estate_BDS/Data_Need_ETL/BDS_Fill_Province.xlsx''')


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

# def fix_province(r):
#     temp = r['province']
#     if 'hcm' in temp or 'ho chi minh' in temp:
#         return 'ho chi m'
#     elif 'vung tau' in temp or 'ba ria - vung tau' in temp:
#         return 'ba ria-vung'
#     elif 'da lat' in temp:
#         return 'lam dong'
#     elif 'quy nhon' in temp:
#         return 'binh dinh'
    
#     else:
#         return temp
# df['province'] = df.apply(fix_province, axis = 1)


#%% change address cols to match key
# def modify_province(r):
#     if r['province'] == 'ho chi minh':
#         return 'ho chi m'
#     elif r['province'] == 'ba ria - vung tau':
#         return 'ba ria-vung'
#     else:
#         return r['province']
# df['province'] = df.apply(modify_province, axis=1)

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




# check_ward_key.to_excel('D:/MOHO - DOANH SỐ/FILE GỐC HARAVAN/TESTING/Fill_na_Province_Apartment.xlsx', index = False) # fill null bằng tay file này


#### D:/MOHO - DOANH SỐ/FILE GỐC HARAVAN/TESTING/CREATE Longtitude, Latitude/MAP-Province-District-Ward.xlsx


#%% fill na PROVINCE, DISTRICT, WARD
fill_na = pd.read_excel(
    'D:/MOHO - DOANH SỐ/FILE GỐC HARAVAN/TESTING/Fill_na_Province_Apartment.xlsx')
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
    print(' -  FILL DATA HERE: D:/MOHO - DOANH SỐ/FILE GỐC HARAVAN/TESTING/Fill_na_Province_Apartment.xlsx')
    print('+ Reference:  D:/MOHO - DOANH SỐ/FILE GỐC HARAVAN/TESTING/CREATE Longtitude, Latitude/MAP-Province-District-Ward.xlsx')

# MORE DETAIL PROJECT NAME:
check_null2 = check_null_address.merge(df_all[['Project_name', 'Project_location']], how = 'left', on = ['Project_location'])


# lst_check = ['Vĩnh Yên, Vĩnh Phúc', 'TP. Nha Trang, Khánh Hòa', 'TP. Hà Tĩnh, Hà Tĩnh', 'TP. Thanh Hóa, Thanh Hóa', 'Quận Hồng Bàng, TP. Hải Phòng', 'Hải Châu, Đà Nẵng']

# check = df.loc[df['Project_location'].isin(lst_check)]

#%%
df.info()
df_all.rename({'Province': 'province_key', 'District': 'district_key'}, axis = 1, inplace = True)
df = df[['Project_location', 'province_key', 'district_key', 'ward_key']]

df_null_o = df.copy()

# df.rename({'province_key': 'province', 'district_key': 'district', 'ward_key': 'ward'}, axis = 1, inplace = True)



for col in list(df.columns):
    df[col] = df[col].replace('None', np.nan)
    df[col] = df[col].replace('nan', np.nan)

# check = df.loc[df['province'] == 'ho chi minh']['district'].value_counts()


# check__province_1 = df[['province_key', 'province']].drop_duplicates()
# check__district_1 = df[['district_key', 'district', 'province']].drop_duplicates()
# check__ward_1 = df[['ward_key', 'ward']].drop_duplicates()

# check__province_1 = df.loc[(df['province'].notnull()) & (df['province_key'].isnull())][['Project_location', 'province', 'province_key']].drop_duplicates()
# check__district_1 = df.loc[(df['district'].notnull()) & (df['district_key'].isnull())][['Project_location', 'district', 'district_key']].drop_duplicates()
# check__ward_1 = df.loc[(df['ward'].notnull()) & (df['ward_key'].isnull())][['Project_location', 'ward', 'ward_key']].drop_duplicates()

#%% CHUYỂN ĐỔI KHÔNG DẤU THÀNH CÓ DẤU, Title giống mapping data:
    
df_map = pd.read_excel('D:/MOHO - DOANH SỐ/FILE GỐC HARAVAN/TESTING/CREATE Longtitude, Latitude/Fix_Province.xlsx')

df_province = df_map[['province', 'province_fix']].drop_duplicates()
df_district = df_map[['province', 'district', 'district_fix']].drop_duplicates() # district duplicated
df_ward = df_map[['province', 'district', 'ward', 'ward_fix']].drop_duplicates() # ward duplicated

# check = df_province['province'].value_counts()


# check_1 = df_district.loc[df_district['district'] == 'thanh tri']

# df2 = df[['province_key']]

# test = df2.merge(df_province, how = 'left', right_on = 'province', left_on = 'province_key').drop_duplicates()


# df_district = df_map[['province', 'province_fix', 'district', 'district_fix']].drop_duplicates()
    

# df_ward = df_map[['province', 'province_fix', 'district', 'district_fix', 'ward', 'ward_fix']].drop_duplicates()

# print('Check DT TỔNG lần 2: {}'.format(df['Tổng tiền sp phụ'].sum()))
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
df_old = pd.read_excel('''D:/MOHO - DOANH SỐ/FILE GỐC HARAVAN/TESTING/CREATE Longtitude, Latitude/MAP_Layer_Apartment_ALL.xlsx''')
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
df_address = pd.read_excel('D:/MOHO - DOANH SỐ/FILE GỐC HARAVAN/TESTING/CREATE Longtitude, Latitude/Apartment_Project_with_full_corr.xlsx')

df = df.merge(df_address, how = 'left', on = 'Project_location')

df_address_null = df.loc[df['Latitude'].isnull()][['Project_location']]

print('Số lượng địa chỉ dự án Apartment null: {}'.format(df_address_null))

# df.to_excel('D:/MOHO - DOANH SỐ/FILE GỐC HARAVAN/TESTING/CREATE Longtitude, Latitude/MAP_Layer_Apartment_ALLinVN.xlsx', sheet_name = 'Apartment_Project',index = False)

#%% FILL TỌA ĐỘ DỰ ÁN:
##UPLOAD Address thiếu TỌA ĐỘ LÊN GGSHEET:
# import unidecode
# import gspread 
# from gspread_dataframe import set_with_dataframe

# gc = gspread.service_account(filename='D:/Credentials_file/project-aftersale-moho-8ae2fb5f1b6b.json')

# # gc = gspread.service_account(filename='D:/FILE DANH SÁCH SP/dulcet-cable-304302-4a41a95a6321.json')
# sh = gc.open_by_key('1C4itMuXyK3_b0D4s-0wXaQuDxSox4GVcz4lCw8cGx8M')
# worksheet = sh.get_worksheet(0)
# worksheet.clear()

# # APPEND DATA TO SHEET
# set_with_dataframe(worksheet, df_address_null)  # NHỚ CHẠY GEO TRÊN GG TRƯỚC KHI TẢI VỀ MERGE LẠI

# # link upload data: 'https://docs.google.com/spreadsheets/d/1C4itMuXyK3_b0D4s-0wXaQuDxSox4GVcz4lCw8cGx8M/edit#gid=0'

# # # #%%%%
# df_address_null = worksheet.get_all_values()
# df_address_null = pd.DataFrame(df_address_null, columns = ['Project_location', 'Latitude', 'Longitude'])  #'Unnamed: 0' 

# df_address_null = df_address_null.iloc[1: , :]

# df_address_null.to_excel('D:/MOHO - DOANH SỐ/FILE GỐC HARAVAN/TESTING/CREATE Longtitude, Latitude/Apartment_Project_Temporary.xlsx', index = False)
# df_address_null = pd.read_excel('D:/MOHO - DOANH SỐ/FILE GỐC HARAVAN/TESTING/CREATE Longtitude, Latitude/Apartment_Project_Temporary.xlsx')


# df_address = pd.read_excel('D:/MOHO - DOANH SỐ/FILE GỐC HARAVAN/TESTING/CREATE Longtitude, Latitude/Apartment_Project_with_full_corr.xlsx')

# df_address = pd.concat([df_address, df_address_null]).drop_duplicates()



# df_address = df_address.loc[df_address['Latitude'].notnull()]


# df_address.to_excel('D:/MOHO - DOANH SỐ/FILE GỐC HARAVAN/TESTING/CREATE Longtitude, Latitude/Apartment_Project_with_full_corr.xlsx', index = False) # mai upload ggs chạy lại

## df_address.to_excel('D:/BACK UP FILE EXCEL SP FOR ETL/Apartment_Project_with_full_corr - BACKUP.xlsx', index = False)

#%% ETL INFO PROJECT: PRICE, NUMBER BUILDING, NUMBER APARTMENT
import ast
df = pd.read_excel('D:/MOHO - DOANH SỐ/FILE GỐC HARAVAN/TESTING/CREATE Longtitude, Latitude/MAP_Layer_Apartment_ALLinVN.xlsx', sheet_name = 'Apartment_Project')

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

# TÁCH LIST TRONG COLS THÀNH NHIỀU VALUE TƯƠNG ỨNG 1 COLS
df3 = pd.DataFrame([pd.Series(x) for x in df1.Info]) # CỘT MATERIALS CHỨA LIST
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

df.to_excel('D:/MOHO - DOANH SỐ/FILE GỐC HARAVAN/TESTING/Data For Tableau Dashboard - Sales By Demographics & GEO/MAP_Layer_Apartment_ALLinVN.xlsx', sheet_name = 'Apartment_Project',index = False)


#%%

df = pd.read_excel('D:/MOHO - DOANH SỐ/FILE GỐC HARAVAN/TESTING/Data For Tableau Dashboard - Sales By Demographics & GEO/MAP_Layer_Apartment_ALLinVN.xlsx', sheet_name = 'Apartment_Project')
df.to_csv('D:/MOHO - DOANH SỐ/FILE GỐC HARAVAN/TESTING/Data For Tableau Dashboard - Sales By Demographics & GEO/MAP_Layer_Apartment_ALLinVN.csv', index = False)


df.columns


check = df[['Project_name', 'Project_location', 'Province', 'District', 'Ward']]

check_NULL_GEO = df.loc[(df['Project_location'].notnull()) & ((df['Province'].isnull()) | (df['District'].isnull()) | (df['Ward'].isnull()))][['Project_location', 'Province', 'District', 'Ward']].sort_values(by = ['Province', 'District'], ascending = [True, False])




df = check_NULL_GEO.copy()
df.rename({'Province': 'province_key', 'District': 'district_key', 'Ward': 'ward_key'}, axis = 1,inplace = True)









