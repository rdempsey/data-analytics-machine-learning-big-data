
# coding: utf-8

# # Take a Random Sample of a Pandas Dataframe

# In[ ]:


# Imports
import pandas as pd
import numpy as np


# In[ ]:


# Location of the data file
data_file = "/Users/robert.dempsey/Dev/daamlobd/data/ontime/flights.csv"


# In[ ]:


# Get the data
flights_df = pd.read_csv(data_file)


# In[ ]:


# View the top five records
flights_df.head(5)


# In[ ]:


# Determine the number of records in the sample
num_records = len(flights_df)
sample_percentage = 0.2
num_records_in_sample = int(num_records * sample_percentage)

print("Total records: {}".format(num_records))
print("Sample percentage: {}%".format(sample_percentage * 100))
print("Records in sample: {}".format(num_records_in_sample))


# In[ ]:


# Create a sample from the dataframe
sample_df = flights_df.sample(num_records_in_sample)


# In[ ]:


# Show the top 10 rows of the sample
sample_df.head()


# In[ ]:


# Records in sample
print("Records in sample dataset: {}".format(len(sample_df)))

