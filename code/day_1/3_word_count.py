
# coding: utf-8

# In[ ]:


# Import what we need
import findspark
findspark.init()

import pyspark
from os import getlogin, path
import operator


# In[ ]:


# Get the current user
current_user = getlogin()

# Create a variable for the home directory to save all the data to
home_dir = path.join("/home", current_user)
data_dir = path.join(home_dir, "data")
word_count_dir = path.join(data_dir, "wordcount")

print("Home Directory: {}".format(home_dir))
print("Data Directory: {}".format(data_dir))
print("Word Count Directory: {}".format(word_count_dir))


# In[ ]:


# Create a Spark context to use
sc = pyspark.SparkContext(appName="WordCount")


# In[ ]:


# Load the text file into an RDD
text_file = sc.textFile("/home/students/data/obama.txt")


# In[ ]:


# Run the word count
counts = text_file.flatMap(lambda line: line.split(" "))              .map(lambda word: (word, 1))              .reduceByKey(lambda a, b: a + b)


# In[ ]:


# Show the count of words we've counted
counts.count()


# In[ ]:


# View 10 of the results
counts.take(10)


# In[ ]:


# Save the results to the data directory
# Note - this will create more than one file containing all the results
if not path.exists(word_count_dir):
    counts.saveAsTextFile(word_count_dir)
    print("Results saved.")
else:
    print("Directory exists. Please delete it before proceeding.")


# ## Working with the results

# In[ ]:


# Convert the word count results into a Python list we can work with.
word_count = counts.collect()


# In[ ]:


# Print some stats
print("Counts: {}".format(len(word_count)))
print("Example count: {}".format(word_count[0]))
print("Data type: {}".format(type(word_count[0])))


# In[ ]:


# Convert the list of tuples to a dictionary
word_count_dict = dict(word_count)

# Sort the dictionary by count
sorted_word_count = sorted(word_count_dict.items(), key=operator.itemgetter(1))

# Show the word and the count
for word in sorted_word_count:
    print("{} - {}".format(word[0], word[1]))


# In[ ]:


# Close the Spark context
sc.stop()

