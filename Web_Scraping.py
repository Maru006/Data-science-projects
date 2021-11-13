from bs4 import BeautifulSoup
import requests
import sqlite3
from collections import Counter
import pandas as pd
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)
import datetime
date = datetime.datetime.now()
from matplotlib import pyplot as plt
import seaborn as sns

url = requests.get(
    'https://www.metoffice.gov.uk/weather/forecast/gcpu7tnmn#?nearestTo=Morden%20(Merton)&date=2021-08-16')
webPageSoup = BeautifulSoup(url.content, 'html.parser')
weatherTabs = webPageSoup.find_all(class_="forecast-tab")
tabDay = [weatherTabs.find(class_='tab-day').get_text().strip().replace('Today', date.strftime('%a-%d-%b')).replace(' ', '-') for weatherTabs in weatherTabs]
lowTemperature = [weatherTabs.find(class_='tab-temp-low').get_text().strip().replace('°', '') for weatherTabs in weatherTabs]
highTemperature = [weatherTabs.find(class_='tab-temp-high').get_text().strip().replace('°', '') for weatherTabs in weatherTabs]
summaryText = [weatherTabs.find(class_='summary-text hide-xs-only').get_text().strip() for weatherTabs in weatherTabs]

df = pd.DataFrame(
    {'Date': tabDay, 'L_Temperature': lowTemperature, 'H_Temperature': highTemperature, 'Notes': summaryText})


connection = sqlite3.connect('C:\\Users\\Maru\\weather_scraping.db')
cursor = connection.cursor()


def create_legend():
    legend_sql = 'CREATE TABLE IF NOT EXISTS weather_legend (date, low_temperature , high_temperature , summary_text)'
    cursor.execute(legend_sql)
    connection.commit()


def insert_values():
    for i, row in df.iterrows():
        insert_to_sql = f'INSERT INTO weather_legend (date, low_temperature, high_temperature, summary_text) VALUES ("{row[0]}", "{row[1]}", "{row[2]}", "{row[3]}")'
        cursor.execute(insert_to_sql)
    connection.commit()


def create_linear():
    linear_sql = 'CREATE TABLE IF NOT EXISTS weather_linear (date, low_temperature, high_temperature, summary_text)'
    cursor.execute(linear_sql)
    connection.commit()


def insert_last_value():
    df_tail = df.tail(1)
    for i, row in df_tail.iterrows():
        insert_to_sql = f'INSERT INTO weather_linear (date, low_temperature, high_temperature, summary_text) VALUES ("{row[0]}", "{row[1]}", "{row[2]}", "{row[3]}")'
        cursor.execute(insert_to_sql)
    connection.commit()


def weather_legend():
    create_legend()
    insert_values()
    print(pd.read_sql('SELECT * FROM weather_legend', connection))


def weather_linear():
    create_linear()
    insert_last_value()
    print(pd.read_sql('SELECT * FROM weather_linear', connection))


weather_legend()
weather_linear()

connection.close()