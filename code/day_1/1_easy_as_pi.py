
# coding: utf-8

# In[ ]:


# Import everything we need
import findspark
findspark.init()

import pyspark
import random


# In[ ]:


# Create a Spark context to use
sc = pyspark.SparkContext(appName="EasyAsPi")


# In[ ]:


# Run the pi example
num_samples = 100000000

def inside(p):     
  x, y = random.random(), random.random()
  return x*x + y*y < 1

count = sc.parallelize(range(0, num_samples)).filter(inside).count()

pi = 4 * count / num_samples

print(pi)


# In[ ]:


# Close the Spark context
sc.stop()

