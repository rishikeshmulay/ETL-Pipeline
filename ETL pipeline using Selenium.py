# -------------------------------------Logging and Downloading Zip File------------------------------------------------------

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

# Start browser
driver = webdriver.Chrome()


driver.get("https://www.kaggle.com/datasets/samithsachidanandan/air-traffic-in-europe-from-2016-to-2024?resource=download")
driver.maximize_window()

#click SignIN button

SignIn="//button[span[text()='Sign In']]"

SignIn_button=WebDriverWait(driver,10).until(
    EC.element_to_be_clickable((By.XPATH,SignIn))
)

SignIn_button.click()

#clicking SignIn with Email

SignIn_Email= "//button[span[contains(text(),'Sign in with Email')]]"

SignIn_Email_button=WebDriverWait(driver,10).until(
    EC.element_to_be_clickable((By.XPATH,SignIn_Email))
)
SignIn_Email_button.click()



#entering mailID and password

#email

email_field = WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.XPATH, "//input[@name='email']"))
)
email_field.send_keys("xxxxx@xxxxxx.com")      #kaggle logging mail ID

#password

password_field = WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.XPATH, "//input[@name='password']"))
)
password_field.send_keys("xxxxx@xxxxx")       #kaggle password



#clicking signin button

signin_button_xpath = "//button[span[text()='Sign In']]"

signin_button = WebDriverWait(driver, 10).until(
    EC.element_to_be_clickable((By.XPATH, signin_button_xpath))
)
signin_button.click()

time.sleep(10)

print("Login Succesful")


#DOWNLOAD Button for Zip file

Download_button_xpath = "//button[span[contains(text(),'Download')]]"

Download_button = WebDriverWait(driver, 10).until(
    EC.element_to_be_clickable((By.XPATH, Download_button_xpath))
)
Download_button.click()

# Clicking to download zip file
download_text_xpath = "//p[contains(text(), 'Download dataset as zip')]"
download_element = WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.XPATH, download_text_xpath))
)
download_element.click()  # If this element is clickable (some sites use links or buttons for actual download)

time.sleep(20)
driver.quit()

print ("File downloaded succesfully")




# ------------------------------------- Opening Zip File from System------------------------------------------------------

import zipfile
import pandas as pd
import os
import glob

#path to downloaded zipfile
download_folder=r'C:\Users\Windows\Downloads'

#List of all .zip files in that folder
zip_files=glob.glob(os.path.join(download_folder, '*.zip'))

#latest zipfolder using file creation time
latest_zip=max(zip_files,key=os.path.getctime)   

#open zip file and read excel file
with zipfile.ZipFile(latest_zip,'r') as z:
    all_files=z.namelist()
    # list all files in zipfiles
    print("CSV", z.namelist())
    
  #Required file
    req_file=[f for f in all_files if 'airport_traffic_2016' in f.lower() and f.endswith('.csv')]
    
    if not req_file:
        raise FileNotFoundError("File is not found in zip folder")
    
    
#Opening and reading and Excel file

    with z.open(req_file[0]) as csv_file:
        df=pd.read_csv(csv_file)
    
      
# ------------------------------------- Data Cleaning in DataFrame ------------------------------------------------------

df=df.fillna(0)
df

# ------------------------------------- Adding Date & Time in Dataframe ------------------------------------------------------
import datetime

from datetime import datetime

now=datetime.now() #get current time

df['Date']=now.strftime('%Y-%m-%d')
df['Time']=now.strftime('%H:%M')

# ------------------------------------- Connecting SQL and create_engine ------------------------------------------------------

from sqlalchemy import create_engine

#database connection info  (creating connection using given details)
user="XXXX"
password="XXXXXXXXXXXX"
host="XXXXXXXXX"
port="XXXXXXXX"
database="XXXXXXX"

#create SQLAlchemy engine
engine=create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}')

#upload to MySQL

df.to_sql(name="dataframe1",con=engine,index=False,if_exists='append')   # (if_exists="replace") need to add if new table has to create 
