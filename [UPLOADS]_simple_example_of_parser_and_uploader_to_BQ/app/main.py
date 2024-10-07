from flask import Flask, send_file, jsonify, request
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import requests
import time
from time import mktime
import pytz
import os
from bs4 import BeautifulSoup
import json
import pandas as pd
import APIClasses
from datetime import datetime, timedelta, date
from dotenv import load_dotenv, find_dotenv
import random
import traceback
from webdriver_manager.chrome import ChromeDriverManager
import chromedriver_autoinstaller


app = Flask(__name__)
load_dotenv(find_dotenv())
chromedriver_autoinstaller.install()


chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("window-size=1024,768")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--enable-javascript")

# Initialize a new browser
browser = webdriver.Chrome(options=chrome_options)

@app.route("/")
def updating():
    try:
        username = str(os.getenv('login'))
        password = str(os.getenv('password'))
        browser.get(str(os.getenv('url')))
        browser.find_element(By.NAME, "email").send_keys(username)
        browser.find_element(By.NAME, "password").send_keys(password)
        browser.find_element(By.CSS_SELECTOR, "[type=submit]").click()
        time.sleep(15 + random.randint(1, 2))

        max_dt = list(APIClasses.SQLQuery(dataset_id = str(os.getenv('dataset_id')), table_id = str(os.getenv('table_id')), destination='WRITE_APPEND').exists_table()['date'])[0]
        data_ = str((datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"))

        url = 'url with date_start and date_end {} {}'.format(max_dt, data_)

        browser.get(url)
        time.sleep(3 + random.randint(1, 2))
        browser.find_element(By.CSS_SELECTOR, "[type=submit]").click()
        time.sleep(3 + random.randint(1, 2))

        soup = BeautifulSoup(browser.page_source, features="html.parser")
        total_urls = soup.find_all() #with need tag and need class

        """logical with parsing of need data with result as pandas DataFrame"""
        df_for_insert =pd.DataFrame()

        result = APIClasses.SQLQuery(dataset_id = str(os.getenv('dataset_id')), table_id = str(os.getenv('table_id')), destination='WRITE_APPEND').insert_to_bq(df_for_insert)
        APIClasses.Tgmessage(chatid = os.getenv('telegram_id'), send = 'Message of tg sucsessfully upload').message_alarm()
        return 'status-200, ok'
    except Exception as e:
        APIClasses.Tgmessage(chatid = os.getenv('telegram_id'), send = str(traceback.format_exc())).message_alarm()
        return 'status-400'


