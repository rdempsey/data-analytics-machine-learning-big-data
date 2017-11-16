
# coding: utf-8

# The following example demonstrates training an elastic net regularized linear regression model and extracting model summary statistics.

# In[ ]:


import findspark
findspark.init()

from os import getlogin, path

from pyspark import SparkContext
from pyspark.sql import SQLContext

from pyspark.ml.regression import LinearRegression


# In[ ]:


# Directories 
MLLIB_DATA_DIR = path.join("/home/students/data/", "mllib")
DATA_FILE   = path.join(MLLIB_DATA_DIR, "sample_linear_regression_data.txt")


# In[ ]:


# Create a SparkContext and a SQLContext context to use
sc = SparkContext(appName="Linear Regression with Spark")
sqlContext = SQLContext(sc)


# In[ ]:


# Load the training data into a dataframe
training = sqlContext.read.format("libsvm").load(DATA_FILE)
training.show()


# In[ ]:


# Create an instance of a LinearRegression model
lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
print(lr)


# In[ ]:


# Fit (train) the model
lr_model = lr.fit(training)
print(lr_model)


# In[ ]:


# Print the coefficients and intercept for linear regression
print("Coefficients: {}".format(lr_model.coefficients))
print("Intercept: {}".format(lr_model.intercept))


# ## Summarize the model over the training set and print out some metrics

# In[ ]:


# Get the model summary
training_summary = lr_model.summary


# In[ ]:


print("numIterations: {}".format(training_summary.totalIterations))


# In[ ]:


print("objectiveHistory")
for h in training_summary.objectiveHistory:
    print(h)


# In[ ]:


training_summary.residuals.show()


# In[ ]:


print("RMSE: %f" % training_summary.rootMeanSquaredError)


# In[ ]:


print("r2: %f" % training_summary.r2)


# In[ ]:


sc.stop()

