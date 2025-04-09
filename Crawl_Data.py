# -*- coding: utf-8 -*-
"""
Created on Fri Aug  4 11:27:10 2023

@author: aston
"""
from IPython import get_ipython
get_ipython().magic('reset -sf') 
import pandas as pd
from datetime import datetime, timedelta
from time import sleep
from selenium import webdriver
from selenium.webdriver.chrome.options import Options 


now1 = datetime.now()
current_time = now1.strftime("%H:%M:%S")
current_date = now1.strftime("%Y/%m/%d")
today = pd.to_datetime('now').strftime("%d.%m.%y")
options = Options()

import os
current_path = os.getcwd()

#%%
lst_p = []
for i in range(0, 4):  # 74, 160
    options.binary_location = current_path + "/data/chrome.exe" # https://stackoverflow.com/questions/45500606/set-chrome-browser-binary-through-chromedriver-in-python
    driver = webdriver.Chrome(chrome_options=options, executable_path= current_path + "/data/chromedriver.exe") # https://chromedriver.chromium.org/home
    driver.set_window_size(1920, 1080)
    
    
    
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
        Project_location = Project_location = item.find_element_by_xpath('.//div[@class="re__prj-card-location"]').text #ok
        
    
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
    des = []
    for infos in item.find_elements_by_xpath('.//div[@class="re__prj-card-info-content"]'):
        info = [item.text for item in infos.find_elements_by_xpath('.//span[@class="re__prj-card-config-value"]')]
        des.append(info)
    summary = summary = item.find_element_by_xpath('.//div[@class="re__prj-card-summary"]').text
    link = link = item.find_element_by_xpath('.//a').get_attribute('href')
 
    dict_p = {'Project_name': Name, 'Status': Project_status, 'Info': des, 'Summary': summary,'Project_location': Project_location, 'Link_project': link}
    lst_p.append(dict_p)
        
        
    driver.quit()  
    c+=1
    c_1+= float(1*100/n_p) 
    print('Product completed: {}, ({:.2f}%)'.format(c, c_1))
df = pd.DataFrame(lst_p)           
df['Source'] = 'batdongsan.com.vn'        
df['Map_layer'] = 'Apartment'
    
df.rename({"Summary": 'project_description'}, axis = 1, inplace = True)
    
df = df.drop_duplicates()

df.to_excel(current_path + str(today) + '.xlsx', index= False) 


import pyttsx3

engine = pyttsx3.init()
voices = engine.getProperty('voices')
engine.setProperty('voice', voices[1].id)
# rate = engine.getProperty('rate')
# engine.setProperty('rate', rate-80)
engine.say(', CRAWLING ESTATE PROJECT COMPLETED'.format(i))
engine.runAndWait()               
            
