import seaborn as sns
from matplotlib import pyplot as plt
import pandas as pd

pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 1000)
import sqlite3
import threading
from collections import Counter
from datetime import datetime

connection = sqlite3.connect('C:\\Users\\Maru\\weather_scraping.db')
cursor = connection.cursor()


def legendPlot():
    legend = pd.read_sql('SELECT * FROM weather_legend', connection)
    legend['date'] = legend['date'] + '-2021'
    legend.loc[:, 'date'] = pd.to_datetime(legend['date'], format='%a-%d-%b-%Y')
    print(legend)

    legend.columns.astype('str')
    legend['low_temperature'] = legend['low_temperature'].astype(float)
    legend['high_temperature'] = legend['high_temperature'].astype(float)
    sns.set_theme()
    sns.lineplot(data=legend, x='date', y='low_temperature', label='Low Temperature')
    sns.lineplot(data=legend, x='date', y='high_temperature', label='High Temperature')
    plt.title('Weather Legend')
    plt.show()


def variability_analysis():
    data_legend = pd.read_sql('SELECT * FROM weather_legend', connection)
    data_legend['date'] = data_legend['date'] + '-2021'
    data_legend.loc[:, 'date'] = pd.to_datetime(data_legend['date'], format='%a-%d-%b-%Y')

    data_legend.columns = data_legend.columns.astype('str')
    data_legend['low_temperature'] = data_legend['low_temperature'].astype(float)
    data_legend = data_legend.assign(frequency=0)
    data_legend['high_temperature'] = data_legend['high_temperature'].astype(float)

    counter = Counter(data_legend['date'])
    keys = list(dict.keys(counter))
    groups = []
    frequencies = []
    reindex = []
    for key in keys:
        groups.append(list(data_legend[data_legend['date'] == key].index))  # groups now returns a list of lists containing index.

    for lists in groups:
        for i in lists:
            reindex.append(i)  # mo now returns a list containing individuals indexes from each lists inside groups.
    data_legend = data_legend.reindex(reindex)

    for indexes in groups:  # 'indexes' returns a list containing indexes.
        for i in range(0, len(indexes), 1):  # this counts the length for each list of indexes
            frequencies.append(i)  # frequencies now return the ranges in ascending maximum len of each indexes

    data_legend = data_legend.assign(frequency=frequencies)
    data_legend['frequency'] = data_legend['frequency'] + 1
    data_legend.sort_index(ascending=True, inplace=True)

    sns.set_theme()
    sns.color_palette()
    facet = sns.FacetGrid(data=data_legend, col='date')
    facet.map(sns.lineplot, 'frequency', 'low_temperature')
    facet.map(sns.lineplot, 'frequency', 'high_temperature')
    plt.tight_layout()
    plt.ylabel('Temperature')
    plt.legend()
    plt.show()

    print(data_legend)


variability_analysis()
connection.close()
