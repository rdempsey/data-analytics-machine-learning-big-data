
# coding: utf-8

# The following example demonstrates training an elastic net regularized linear regression model and extracting model summary statistics.

# In[ ]:


import findspark
findspark.init()

from os import getlogin, path

from pyspark import SparkContext
from pyspark.sql import SQLContext

from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


# In[ ]:


# Directories 

# HOME_DIR = path.join("/home", getlogin())
HOME_DIR = path.join("/Users/robert.dempsey/Dev/daamlobd")
DATA_DIR = path.join(HOME_DIR, "data")
MLLIB_DATA_DIR = path.join(DATA_DIR, "mllib")
DATA_FILE   = path.join(MLLIB_DATA_DIR, "sample_libsvm_data.txt")

# Check the things
print("Home Directory: {}".format(HOME_DIR))
print("Data Directory: {}".format(DATA_DIR))
print("MLlib Data Directory: {}".format(MLLIB_DATA_DIR))
print("Data File: {}".format(DATA_FILE))


# In[ ]:


# Create a SparkContext and a SQLContext context to use
sc = SparkContext(appName="Naive Bayes Classification with Spark")
sqlContext = SQLContext(sc)


# In[ ]:


# Load the training data
data = sqlContext.read.format("libsvm").load(DATA_FILE)
data.show()


# In[ ]:


# Split the data into train and test
splits = data.randomSplit([0.6, 0.4], 1234)
train = splits[0]
test = splits[1]

train.show(5)


# In[ ]:


# Create the trainer and set its parameters
nb = NaiveBayes(smoothing=1.0, modelType="multinomial")
print(nb)


# In[ ]:


# train the model
nb_model = nb.fit(train)
print(nb_model)


# In[ ]:


# select example rows to display.
predictions = nb_model.transform(test)
predictions.show()


# In[ ]:


# compute accuracy on the test set
evaluator = MulticlassClassificationEvaluator(labelCol="label",
                                              predictionCol="prediction",
                                              metricName="accuracy")

accuracy = evaluator.evaluate(predictions)
print("Test set accuracy = {}".format(accuracy))


# ## Close it down

# In[ ]:


sc.stop()

