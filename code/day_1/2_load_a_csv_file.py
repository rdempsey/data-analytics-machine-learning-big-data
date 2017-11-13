
# coding: utf-8

# In[ ]:


# Import what we need
import findspark
findspark.init()

import pyspark


# In[ ]:


# Create a Spark context to use
sc = pyspark.SparkContext(appName="LoadCSVFile")


# In[ ]:


# Load the CSV file into an RDD
text_file = sc.textFile("/home/students/data/earthquakes.csv")


# In[ ]:


# Print the first line of the csv file
print(text_file.first())


# In[ ]:


# Get the line count
text_file.count()


# In[ ]:


# Close the Spark context
sc.stop()

