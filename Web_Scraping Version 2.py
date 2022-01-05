import logging

logging.basicConfig(level=logging.DEBUG,
                    format='%(message)s',
                    filename='%slog' % __file__[:-2])
import datetime

date = datetime.datetime.now()
from bs4 import BeautifulSoup
import requests
import sqlite3
import pandas as pd

pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)

url = 'https://www.metoffice.gov.uk/weather/forecast/gcpu7tnmn#?nearestTo=Morden%20&date=2022-01-04'
text = requests.get(url)

# Html parsed
soup = BeautifulSoup(text.content, 'lxml')  # python dependent html.parser issues when parsing .get methods hence lxml
forecasts = soup.find_all(name='li', attrs={'class': 'forecast-tab'})  # returns the list of 'tabDay[0-6]'
tabDay = [forecasts.find(name='time').get('datetime') for forecasts in forecasts]
lowTemperature = [forecasts.find(name='span', attrs={'class': 'tab-temp-low'}).get('data-value') for forecasts in forecasts]
highTemperature = [forecasts.find(name='span', attrs={'class': 'tab-temp-high'}).get('data-value') for forecasts in forecasts]
summaryText = [forecasts.find(name='div', attrs={'class': "summary-text hide-xs-only"}).get_text().strip() for forecasts in forecasts]

df = pd.DataFrame(
    {'Date': tabDay, 'L_Temperature': lowTemperature, 'H_Temperature': highTemperature, 'Notes': summaryText})

# creates a database in the system. Pass your own directory
connection = sqlite3.connect('weather_scraping.db')  # change this directory for your database
cursor = connection.cursor()


# creates the table
def create_legend():
    legend_sql = 'CREATE TABLE IF NOT EXISTS weather_legend (date, low_temperature , high_temperature , summary_text)'
    cursor.execute(legend_sql)


# fills the weather_legend table wth forecast for each day starting Today to 6 days into the future.
# this would be carried out each day, which would naturally fill duplicates of previous forecast. Nevertheless, it is the key to recording variations in weather forecasts.
# analysis can be run with enough data: (ideally, where insert_values has been run each day for a month or so) 10 or more datetime coordinates with their 5 forecasts + 1 actual observation (forecast of the day for that day).
def insert_values():
    for i, row in df.iterrows():
        insert_to_sql = f'INSERT INTO weather_legend (date, low_temperature, high_temperature, summary_text) VALUES ("{row[0]}", "{row[1]}", "{row[2]}", "{row[3]}")'
        cursor.execute(insert_to_sql)
    print(pd.read_sql('SELECT * FROM weather_legend', connection))


# this creates a separate table with only linear information (no recurring dates)
def create_linear():
    linear_sql = 'CREATE TABLE IF NOT EXISTS weather_linear (date, low_temperature, high_temperature, summary_text)'
    cursor.execute(linear_sql)


# only the 6th latest date is including, where it can be used as reference when comparing actual observed vs the 6th day forecast
def insert_last_value():
    df_tail = df.tail(1)
    for i, row in df_tail.iterrows():
        insert_to_sql = f'INSERT INTO weather_linear (date, low_temperature, high_temperature, summary_text) VALUES ("{row[0]}", "{row[1]}", "{row[2]}", "{row[3]}")'
        cursor.execute(insert_to_sql)
    print(pd.read_sql('SELECT * FROM weather_linear', connection))


if __name__ == '__main__':
    create_legend()
    insert_values()
    create_linear()
    insert_last_value()
    connection.close()
    date = datetime.datetime.now().strftime('%d/%m/%Y')
    logging.info(f'{__file__} Last run-time: {date}')
