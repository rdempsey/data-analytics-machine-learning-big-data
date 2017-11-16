
# coding: utf-8

# In[ ]:


# Imports
import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.sql import SQLContext


# In[ ]:


# Location of the data file
data_file = "/home/students/data/ontime/flights.csv"


# In[ ]:


# Create a SparkContext and a SQLContext context to use
sc = SparkContext(appName="Sampling Percentage PySpark")
sqlContext = SQLContext(sc)


# In[ ]:


# Load the data - creates a Spark dataframe
parking_file = "/home/students/data/sf_parking/sf_parking_clean.json"
parking_df = sqlContext.read.json(parking_file)
parking_df.show(10)


# In[ ]:


# Determine the number of records in the sample
num_records = parking_df.count()
sample_percentage = 0.8
num_records_in_sample = int(num_records * sample_percentage)

print("Total records: {}".format(num_records))
print("Sample percentage: {}%".format(sample_percentage * 100))
print("Records in sample: {}".format(num_records_in_sample))


# In[ ]:


# Create new dataframes for train and test datasets using the sample method
sample_df = parking_df.sample(False, sample_percentage, 42)


# In[ ]:


print("Sample records: {}".format(sample_df.count()))


# In[ ]:


sample_df.show(10)


# In[ ]:


sc.stop()

