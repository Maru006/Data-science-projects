import logging
logging.basicConfig(level=logging.DEBUG)
import pandas as pd
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 1000)
import sqlite3


def update(url, table, ID, setVar, setVal):
    connection = sqlite3.connect(url)
    cursor = connection.cursor()
    erroneousID = []
    if ID:
        try:
            iter(ID)
            for i in ID:
                if isinstance(i, int):
                    logging.info(f' {i} is OK')
                    try:
                        cursor.execute(f'UPDATE {table} '
                                       f'SET {setVar} = {setVal} WHERE ROWID = {ID}')
                    except ValueError or TypeError:
                        break
                else:
                    logging.warning(f' {i}  is not an int.')
                    erroneousID.append(i)
                    break
            else:
                if len(erroneousID) == 1:
                    logging.warning(' To update cells, specify ROWID only with an int')
                else:
                    logging.warning(f'{erroneousID} are not integers.'
                                    f' To update cells, specify ROWID only with an int')
        except TypeError:
            logging.warning(f' {ID} must be an int or iterable.'
                            f'To update cells, specify ROWID')
    connection.commit()
    print(pd.read_sql(f'SELECT * FROM {table}', connection))
    connection.close()


def delete_cells(url, table, ID):
    connection = sqlite3.connect(url)
    cursor = connection.cursor()
    erroneousID = []
    if ID:
        try:
            iter(ID)
            for i in ID:
                if isinstance(i, int):
                    logging.info(f' {i} is OK')
                    try:
                        cursor.execute(f'DELETE FROM {table} '
                                       f'WHERE ROWID = {ID})')
                    except ValueError or TypeError:
                        break
                else:
                    logging.warning(f' {i}  is not an int.')
                    erroneousID.append(i)
                    break
            else:
                if len(erroneousID) == 1:
                    logging.warning(' To update cells, specify ROWID only with an int')
                else:
                    logging.warning(f'{erroneousID} are not integers.'
                                    f' To update cells, specify ROWID only with an int')
        except TypeError:
            logging.warning(f' {ID} must be an int or iterable.'
                            f'To update cells, specify ROWID')
    connection.commit()
    print(pd.read_sql(f'SELECT * FROM {table}', connection))
    connection.close()
