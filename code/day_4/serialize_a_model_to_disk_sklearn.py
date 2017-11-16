
# coding: utf-8

# In[ ]:


# Findspark
import findspark
findspark.init()

# Python core libs
from os import getlogin, path
import pandas as pd

# Save Model Using Pickle
import pandas as pd
from sklearn import model_selection
from sklearn.linear_model import LogisticRegression
import pickle


# In[ ]:


# Directories 

# HOME_DIR = path.join("/home", getlogin())
HOME_DIR = path.join("/Users/robert.dempsey/Dev/daamlobd")
DATA_DIR = path.join(HOME_DIR, "data")
MLLIB_DATA_DIR = path.join(DATA_DIR, "mllib")
SKLEARN_DATA_DIR = path.join(DATA_DIR, "sklearn")
DATA_FILE   = path.join(DATA_DIR, "pima-indians-diabetes.data")
PICKLE_FILE = path.join(SKLEARN_DATA_DIR, "lr_model_sklearn.pkl")


# In[ ]:


# Get the data
names = ['preg', 'plas', 'pres', 'skin', 'test', 'mass', 'pedi', 'age', 'class']
df = pd.read_csv(DATA_FILE, names=names)

df.head()


# In[ ]:


# Set up our training and testing sets
arr = df.values
X = arr[:,0:8]
Y = arr[:,8]
test_size = 0.33
seed = 7
X_train, X_test, Y_train, Y_test = model_selection.train_test_split(X, Y, test_size=test_size, random_state=seed)


# In[ ]:


# Fit the model on the 33%
lr_model = LogisticRegression()
lr_model.fit(X_train, Y_train)


# ## Serialize the Model to Disk

# In[ ]:


# Save the model to disk
pickle.dump(lr_model, open(PICKLE_FILE, 'wb'))


# ## Deserialize the Model

# In[ ]:


new_lr_model = pickle.load(open(PICKLE_FILE, 'rb'))
new_lr_model

