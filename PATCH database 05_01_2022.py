import datetime
import logging

logging.basicConfig(level=logging.DEBUG)
import pandas as pd
import pandas.io.sql

pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 1000)
import sqlite3
import re


def update(url, table, setVar, setVal, setID):
    try:
        connection = sqlite3.connect(url)
        cursor = connection.cursor()
        try:
            cursor.execute(f'UPDATE {table} SET {setVar} = ? WHERE ROWID = ?', (setVal, setID))
            connection.commit()
        except sqlite3.Error as error:
            print(f'Error: \n {error}')
            ...
            cursor.execute("SELECT name "
                           "FROM sqlite_master "
                           "WHERE type='table'")
            logging.info(f' In database, "{url}", your existing tables are: {cursor.fetchall()}. If not shown, '
                         f'your url may not exist or is incorrect')
            table_info = pd.read_sql(f'SELECT ROWID, * FROM {table}', connection)
            logging.info(f' In "{table}", your existing columns are: {table_info.columns}. If not shown, '
                         f'your table may not exist or is incorrect')
        logging.info('If the table does not update, note that the ROWID (which may need to be reset) may not exist')
        print(pd.read_sql(f'SELECT ROWID, * FROM {table}', connection))
        connection.close()
        logging.info(' SUCCESSFUL :: Disconnected to Database')
    except pandas.io.sql.DatabaseError:
        logging.warning(f' Could not connect to {url}. If INFO: your existing tables are not shown, this means your url may not exist or is incorrect')


def patch(var_ROWID, var_Date):
    for row, dates in zip(var_ROWID, var_Date):  # tuple
        for i, x in zip(row, dates):  # strings
            try:
                weekdays = r'^...'
                regex = r'...-[\d\d]{1,}-Jan'
                new_year = re.findall(regex, x)
                for match in new_year:
                    updated_dates = f'{"2022"}{re.sub(weekdays, "", match)}'
                    date_object = datetime.datetime.strptime(updated_dates, '%Y-%d-%b').strftime('%Y-%m-%d')
                    update('C:\\Users\\Maru\\weather_scraping.db', 'weather_linear', 'date', date_object, i)
                regex = r'...-[\d\d]{1,}-[^J][^a][^n]'
                past_year = re.findall(regex, x)
                for match in past_year:
                    updated_dates = f'{"2021"}{re.sub(weekdays, "", match)}'
                    date_object = datetime.datetime.strptime(updated_dates, '%Y-%d-%b').strftime('%Y-%m-%d')
                    update('C:\\Users\\Maru\\weather_scraping.db', 'weather_linear', 'date', date_object, i)
            except TypeError:
                pass


if __name__ == '__main__':
    def patch_weather_legend():
        connection = sqlite3.connect('weather_scraping.db') # where your database was created
        cursor = connection.cursor()
        ROWID = cursor.execute('SELECT ROWID FROM weather_legend').fetchall()
        Date = cursor.execute('SELECT Date FROM weather_legend').fetchall()
        patch(ROWID, Date)
        connection.close()


    def patch_weather_linear():
        connection = sqlite3.connect('weather_scraping.db') # the same database as previously above
        cursor = connection.cursor()
        ROWID = cursor.execute('SELECT ROWID FROM weather_linear').fetchall()
        Date = cursor.execute('SELECT Date FROM weather_linear').fetchall()
        patch(ROWID, Date)
        connection.close()
