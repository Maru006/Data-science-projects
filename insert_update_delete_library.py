import logging
logging.basicConfig(level=logging.DEBUG)
import pandas as pd
import pandas.io.sql
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 1000)
import sqlite3

# this let you insert data manually.
def insert(url, table, date, low_temperature, high_temperature, summary_text):
    try:
        connection = sqlite3.connect(url)
        cursor = connection.cursor()
        try:
            cursor.execute(f'INSERT INTO {table} (date, low_temperature, high_temperature, summary_text) '
                           f'VALUES ("{date}", "{low_temperature}", "{high_temperature}", "{summary_text}")')
            connection.commit()
        except sqlite3.OperationalError as error:
            print(f'Error: \n {error}')
            logging.warning(f' database: {url} or table: {table} may not exist. Check info below:')
            cursor.execute("SELECT name "
                           "FROM sqlite_master "
                           "WHERE type='table'")
            logging.info(f' In database, "{url}", your existing tables are: {cursor.fetchall()}. If not shown, '
                         f'your url may not exist or is incorrect')
            table_info = pd.read_sql(f'SELECT ROWID, * FROM {table}', connection)
            logging.info(f' In "{table}", your existing columns are: {table_info.columns}. If not shown, '
                         f'your table may not exist or is incorrect')
        print(pd.read_sql(f'SELECT ROWID, * FROM {table}', connection))
        connection.close()
        logging.info(' SUCCESSFUL :: Disconnected to Database')
    except pandas.io.sql.DatabaseError:
        logging.warning(f' Could not connect to {url}. If INFO: your existing tables are not shown, this means your url may not exist or is incorrect')

    
# this lets you update data manually.
def update(url, table, setVar, setVal, setID):
    try:
        connection = sqlite3.connect(url)
        cursor = connection.cursor()
        try:
            cursor.execute(f'UPDATE {table} SET {setVar} = {setVal} WHERE ROWID = {setID}')
            connection.commit()
        except sqlite3.Error as error:
            print(f'Error: \n {error}')
            logging.warning(f' database: {url}, table: {table} or column: {setVar} may not exist. Check info below:')
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


# this lets you delete faulty data by their ROWID.
def delete(url, table, setID):
    try:
        connection = sqlite3.connect(url)
        cursor = connection.cursor()
        try:
            if isinstance(setID, int):
                cursor.execute(f'DELETE FROM {table} WHERE ROWID = {setID}')
            else:
                logging.warning(f' {setID} is a {type(setID)}. Value must be a unit of int. '
                                f'Try enclosing your argument inside an iterable function instead.')
        except sqlite3.OperationalError as error:
            print(f'Error: \n {error}')
            logging.warning(f' Table: {table} may not exist')
            cursor.execute("SELECT name "
                           "FROM sqlite_master "
                           "WHERE type='table'")
            logging.info(f' Your existing tables are: {cursor.fetchall()}')
        connection.commit()
        logging.info('If the table does not update, note that the ROWID may not exist.')
        print(pd.read_sql(f'SELECT ROWID, * FROM {table}', connection))
        connection.close()
        logging.info(' SUCCESSFUL :: Disconnected to Database')
    except pandas.io.sql.DatabaseError:
        logging.warning(f' Could not connect to {url}. If INFO: your existing tables are not shown, this means your url may not exist or is incorrect')

