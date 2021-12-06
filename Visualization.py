# visualizing data gathered from Web_Scraping.py
# ensure plenty of data before engaging with this material
import seaborn as sns

sns.set_theme()
from matplotlib import pyplot as plt
import pandas as pd

pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 1000)
import sqlite3
import threading
from collections import Counter

# connecting to the previous database
connection = sqlite3.connect('***WHERE THE DATABASE URL WAS CREATED***')
cursor = connection.cursor()

# the following processing turns raw information into a manageable data set for analysis.
# turning a web-scraped 'date' attribute into a date-time object.
data_legend = pd.read_sql('SELECT * FROM weather_legend', connection)
data_legend['date'] = data_legend['date'] + '-2021'
data_legend.loc[:, 'date'] = pd.to_datetime(data_legend['date'], format='%a-%d-%b-%Y')

data_legend.columns = data_legend.columns.astype('str')
data_legend['low_temperature'] = data_legend['low_temperature'].astype(float)
data_legend = data_legend.assign(frequency=0)
data_legend['high_temperature'] = data_legend['high_temperature'].astype(float)

# grouping date times
# date times are attributed to n(days)-1 forecast in advance, with 1-1 (0) as actual observation
counter = Counter(data_legend['date'])
keys = list(dict.keys(counter))
groups = []
frequencies = []
reindex = []
for key in keys:
    groups.append(list(data_legend[data_legend['date'] == key].index))  # groups now returns a list of lists containing index.

for lists in groups:
    for i in lists:
        reindex.append(i)  # reindex now returns a list containing individuals indexes from each lists inside groups.
data_legend = data_legend.reindex(reindex)

for indexes in groups:  # 'indexes' returns a list containing indexes.
    for i in range(0, len(indexes), 1):  # this counts the length for each list of indexes
        frequencies.append(i)  # frequencies now return the ranges in ascending maximum len of each indexes
data_legend = data_legend.assign(frequency=frequencies)
data_legend['frequency'] = data_legend['frequency'] + 1
data_legend.sort_index(ascending=True, inplace=True)  # sorting index to represent the order of record acquisition.


# run this section for a complete visualization of forecasts including incomplete records of date-series
# a line plot which shows the average variation between predicted and actual: lowest and highest temperature.
def legendPlot():
    print(data_legend)
    data_legend['low_temperature'] = data_legend['low_temperature'].astype(float) # may not be necessary
    data_legend['high_temperature'] = data_legend['high_temperature'].astype(float) # may not be necessary

    sns.lineplot(data=data_legend, x='date', y='low_temperature', marker='o',label='Lowest Temperature')
    sns.lineplot(data=data_legend, x='date', y='high_temperature', marker='o', label='Highest Temperature')

    plt.annotate('Shaded areas show aggregates of recorded and predicted temperatures'',
                 xy=(data_legend['date'][7], 15),
                 fontsize=8)
    plt.xlabel('Dates')
    plt.ylabel('Temperature')
    plt.title('Mean-Legend temperature readings')
    plt.show()


# partitions dates which have 7 total observations from dates such as today and 6 days in advance that have only 1 variation
sevens = data_legend[data_legend['frequency'] == 7]
sevens_date = list(sevens['date'])
sevens_indexes = []
for i in sevens_date:
    sevens_index = data_legend[data_legend['date'] == i].index
    sevens_indexes.append(list(sevens_index))
sevens_indexes = [item for sublist in sevens_indexes for item in sublist]  # indexes containing dates with complete records.
data = data_legend.loc[sevens_indexes]
data.sort_index(inplace=True)
sevens_1 = data[data['frequency'] == 1]
sevens_7 = data[data['frequency'] == 7]


# run these section for weather-variability with complete forecast n=7, N=77
# a boxplot difference between 6-day forecast temperatures vs actual temperature (low and high)
def box_predicted_7vs1_actual():
    fig, [ax1, ax2] = plt.subplots(nrows=2, ncols=1)
    fig.supylabel('Temperature')
    fig.suptitle('Difference between Forecast and Actual recorded temperature')

    ax1.boxplot([sevens_7['high_temperature'], sevens_1['high_temperature']])
    ax1.set_title('Highest Temperature')
    ax1.set_xticklabels(labels=[])

    ax2.boxplot([sevens_7['low_temperature'], sevens_1['low_temperature']])
    ax2.set_title('Lowest Temperature')
    ax2.set_xticklabels(labels=['Predicted', 'Actual'])

    plt.annotate('predicted temperatures are 6 days in advance',
                 xy=(1.05, 6),
                 fontsize=8)
    plt.subplots_adjust(hspace=.05)
    plt.tight_layout()
    plt.show()


# a similar comparison over-time.
def line_predicted_7vs1_actual():
    fig, [ax1, ax2] = plt.subplots(nrows=2, ncols=1)
    fig.supylabel('Temperature')
    fig.supxlabel('Dates')
    fig.suptitle('Contrast between Forecast and Actual recorded temperature')

    print(sevens_1)
    ax1.plot(sevens_1['date'], sevens_1['low_temperature'], '.-', label='Actual')
    ax1.plot(sevens_7['date'], sevens_7['low_temperature'], '.-', label='Predicted')
    ax1.set_title('Lowest Temperature')
    ax1.set_xticklabels(labels=[])
    ax1.legend()

    ax2.plot(sevens_1['date'], sevens_1['high_temperature'], '.-')
    ax2.plot(sevens_7['date'], sevens_7['high_temperature'], '.-')
    ax2.set_title('Highest Temperature')

    plt.annotate('predicted temperatures are 6 days in advance',
                 xy=(sevens_1['date'][27], 10),
                 fontsize=10)
    plt.subplots_adjust(hspace=.05)
    plt.tight_layout()
    plt.show()


legendPlot()
box_predicted_7vs1_actual()
line_predicted_7vs1_actual()
connection.close()

