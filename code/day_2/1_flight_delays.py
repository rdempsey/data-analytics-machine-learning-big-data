
# coding: utf-8

# In[ ]:


import findspark
findspark.init()

from os import getlogin, path
from io import StringIO
import csv
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')

from datetime import datetime
from collections import namedtuple
from operator import add, itemgetter
from pyspark import SparkContext


# In[ ]:


# Module Constants
HOME_DIR = path.join("/home", getlogin())
DATA_DIR = path.join(HOME_DIR, "data")
FLIGHT_DELAY_DIR = path.join(DATA_DIR, "flightdelay")
DATE_FMT = "%Y-%m-%d"
TIME_FMT = "%H%M"

print("Home Directory: {}".format(HOME_DIR))
print("Data Directory: {}".format(DATA_DIR))
print("Flight Delay Directory: {}".format(FLIGHT_DELAY_DIR))

fields   = ('date', 'airline', 'flightnum', 'origin', 'dest', 'dep',
            'dep_delay', 'arv', 'arv_delay', 'airtime', 'distance')

Flight   = namedtuple('Flight', fields)


# In[ ]:


# Create a Spark context to use
sc = SparkContext(appName="Flight Delay Analysis")


# In[ ]:


def split(line):
    """
    Operator function for splitting a line with csv module
    """
    reader = csv.reader(StringIO(line))
    return next(reader)


# In[ ]:


def parse(row):
    """
    Parses a row and returns a named tuple.
    """

    row[0]  = datetime.strptime(row[0], DATE_FMT).date()
    row[5]  = datetime.strptime(row[5], TIME_FMT).time()
    row[6]  = float(row[6])
    row[7]  = datetime.strptime(row[7], TIME_FMT).time()
    row[8]  = float(row[8])
    row[9]  = float(row[9])
    row[10] = float(row[10])
    return Flight(*row[:11])


# In[ ]:


# Load the airlines lookup dictionary
airline_file = "/home/students/data/ontime/airlines.csv"
airlines = dict(sc.textFile(airline_file).map(split).collect())


# In[ ]:


# Broadcast the lookup dictionary to the cluster
airline_lookup = sc.broadcast(airlines)


# In[ ]:


# Read the CSV data into an RDD
flights_file = "/home/students/data/ontime/flights.csv"
flights = sc.textFile(flights_file).map(split).map(parse)


# In[ ]:


# Map the total delay to the airline (joined using the broadcast value)
delays  = flights.map(lambda f: (airline_lookup.value[f.airline],
                                 add(f.dep_delay, f.arv_delay)))


# In[ ]:


# Reduce the total delay for the month to the airline
delays  = delays.reduceByKey(add).collect()
delays  = sorted(delays, key=itemgetter(1))


# In[ ]:


# Provide output from the driver
for d in delays:
    print("{} minutes delayed \t {}".format(d[1], d[0]))


# In[ ]:


def plot(delays):
    """
    Show a bar chart of the total delay per airline
    """
    airlines = [d[0] for d in delays]
    minutes  = [d[1] for d in delays]
    index    = list(range(len(airlines)))

    fig, axe = plt.subplots()
    bars = axe.barh(index, minutes)

    # Add the total minutes to the right
    for idx, air, min in zip(index, airlines, minutes):
        if min > 0:
            bars[idx].set_color('#d9230f')
            axe.annotate(" %0.0f min" % min, xy=(min+1, idx+0.5), va='center')
        else:
            bars[idx].set_color('#469408')
            axe.annotate(" %0.0f min" % min, xy=(10, idx+0.5), va='center')

    # Set the ticks
    ticks = plt.yticks([idx+ 0.5 for idx in index], airlines)
    xt = plt.xticks()[0]
    plt.xticks(xt, [' '] * len(xt))

    # Minimize chart junk
    plt.grid(axis = 'x', color ='white', linestyle='-')

    plt.title('Total Minutes Delayed per Airline')
    plt.show()


# In[ ]:


# Show a bar chart of the delays
plot(delays)


# In[ ]:


# Shut it down
sc.stop()

