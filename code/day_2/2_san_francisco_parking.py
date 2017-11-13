
# coding: utf-8

# In[ ]:


import findspark
findspark.init()

from os import getlogin, path

from pyspark import SparkContext
from pyspark.sql import SQLContext


# In[ ]:


# Module Constants
HOME_DIR = path.join("/home", getlogin())
DATA_DIR = path.join(HOME_DIR, "data")
SANFRAN_PARKING_DIR = path.join(DATA_DIR, "sanfranparking")

print("Home Directory: {}".format(HOME_DIR))
print("Data Directory: {}".format(DATA_DIR))
print("SanFran Parking Directory: {}".format(SANFRAN_PARKING_DIR))


# In[ ]:


# Create a SparkContext and a SQLContext context to use
sc = SparkContext(appName="SanFran Parking")
sqlContext = SQLContext(sc)


# In[ ]:


# Load the data - creates a Spark dataframe
parking_file = "/home/students/data/sf_parking/sf_parking_clean.json"
parking = sqlContext.read.json(parking_file)
print(type(parking))


# In[ ]:


# Show 10 rows
parking.show(10)


# ## Examine the Schema and Change Data Types

# In[ ]:


# Examine the schema
parking.printSchema()


# In[ ]:


# Method to convert columns to a new type
def convert_column(df, col, new_type):
    old_col = '%s_old' % col
    df = df.withColumnRenamed(col, old_col)
    df = df.withColumn(col, df[old_col].cast(new_type))
    df = df.drop(old_col)
    return df

# Columns to convert
int_columns = ['regcap', 'valetcap', 'mccap']

# Convert the columns
for col in int_columns:
    parking = convert_column(parking, col, 'int')


# In[ ]:


# Show the new schema
parking.printSchema()


# In[ ]:


# Show 10 rows
parking.show(10)


# ## Create and Query a Table

# In[ ]:


parking.registerTempTable("park")


# In[ ]:


# Run a SQL query against the table
aggr_by_type = sqlContext.sql("SELECT primetype, secondtype, count(1) AS count, round(avg(regcap), 0) AS avg_spaces " +
                              "FROM park " +
                              "GROUP BY primetype, secondtype " +
                              "HAVING trim(primetype) != '' " +
                              "ORDER BY count DESC")

aggr_by_type.show(10)


# We can rewrite the SQL query in the previous example by chaining several simple DataFrame operations.

# In[ ]:


from pyspark.sql import functions as F

aggr_by_type = parking.select("primetype", "secondtype", "regcap")     .where("trim(primetype) != ''")     .groupBy("primetype", "secondtype")     .agg(
        F.count("*").alias("count"),
        F.round(F.avg("regcap"), 0).alias("avg_spaces")
        ) \
    .sort("count", ascending=False)

aggr_by_type.show(10)


# ## Using Describe and Crosstab to Summarize Data

# In[ ]:


# Run describe - like in Pandas
parking.describe("regcap", "valetcap", "mccap").show()


# In[ ]:


# Use crosstab
parking.stat.crosstab("owner", "primetype").show(10)


# ## Add Neighborhood Name

# Define another function that will take a “location_1” struct type and use Google’s Geocoding API to perform a lookup on the latitude and longitude to return the neighborhood name.

# In[ ]:


import requests

def to_neighborhood(location):
    """
    Uses Google's Geocoding API to perform a reverse-lookup on latitude and
    longitude
    https://developers.google.com/maps/documentation/geocoding/
    intro#reverse-example
    """
    name = 'N/A'
    lat = location.latitude
    long = location.longitude

    r = requests.get(
        'https://maps.googleapis.com/maps/api/geocode/json?latlng=%s,%s' %(lat, long))

    if r.status_code == 200:
        content = r.json()
        # results is a list of matching places
        places = content['results']
        neighborhoods = [p['formatted_address'] for p in places if
        'neighborhood' in p['types']]

    if neighborhoods:
        # Addresses are formatted as Japantown, San Francisco, CA
        # so split on comma and just return neighborhood name
        name = neighborhoods[0].split(',')[0]

    return name


# The pyspark.sql.functions module provides the udf function to register a user-defined function (UDF). We declare an inline UDF by passing UDF a callable Python function and the Spark SQL data type that corresponds to the return type.
# 
# In this case, we are returning a string so we will use the StringType data type from pyspark.sql.types. Once registered, we can use the UDF to reformat the “location_1” column with a withColumn expression:

# In[ ]:


from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

location_to_neighborhood=udf(to_neighborhood, StringType())

sfmta_parking = parking.filter(parking.owner == 'SFMTA')     .select("location_1", "primetype", "landusetyp","garorlot", "regcap", "valetcap", "mccap")     .withColumn("location_1",location_to_neighborhood("location_1"))     .sort("regcap", ascending=False)

sfmta_parking.show()


# In[ ]:


# Shut it down
sc.stop()

