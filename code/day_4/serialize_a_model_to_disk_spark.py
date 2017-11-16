
# coding: utf-8

# The following example demonstrates training an elastic net regularized linear regression model and extracting model summary statistics and saving the model to disk.

# In[ ]:


import findspark
findspark.init()

from os import getlogin, path
import pickle

from pyspark import SparkContext
from pyspark.sql import SQLContext

from pyspark.ml.regression import LinearRegression, LinearRegressionModel


# In[ ]:


# Directories 

# HOME_DIR = path.join("/home", getlogin())
HOME_DIR = path.join("/Users/robert.dempsey/Dev/daamlobd")
DATA_DIR = path.join(HOME_DIR, "data")
MLLIB_DATA_DIR = path.join(DATA_DIR, "mllib")
DATA_FILE   = path.join(MLLIB_DATA_DIR, "sample_linear_regression_data.txt")
MODEL_FILE_PATH = path.join(DATA_DIR, "linear_model")

# Check the things
print("Home Directory: {}".format(HOME_DIR))
print("Data Directory: {}".format(DATA_DIR))
print("MLlib Data Directory: {}".format(MLLIB_DATA_DIR))
print("Data File: {}".format(DATA_FILE))
print("Model File Path: {}".format(MODEL_FILE_PATH))


# In[ ]:


# Create a SparkContext and a SQLContext context to use
sc = SparkContext(appName="Linear Regression with Spark")
sqlContext = SQLContext(sc)


# In[ ]:


# Load the training data into a dataframe
training = sqlContext.read.format("libsvm").load(DATA_FILE)


# In[ ]:


# Create an instance of a LinearRegression model
lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)


# In[ ]:


# Fit (train) the model
lr_model = lr.fit(training)
lr_model


# In[ ]:


# Show some summary
lr_model.coefficients


# ## Serialize the Model

# In[ ]:


lr_model.write().overwrite().save(MODEL_FILE_PATH)


# ## Deserialize the model

# In[ ]:


new_lr_model = LinearRegressionModel.load(MODEL_FILE_PATH)
print(new_lr_model)


# In[ ]:


# Show some summary
new_lr_model.coefficients


# ## Clean Up

# In[ ]:


sc.stop()

