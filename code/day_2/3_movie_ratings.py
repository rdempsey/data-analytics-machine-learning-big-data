
# coding: utf-8

# In[ ]:


import findspark
findspark.init()

from os import getlogin, path
from collections import namedtuple

from pyspark import SparkContext
from pyspark.sql import SparkSession


# In[ ]:


# Module constants
HOME_DIR = path.join("/home", getlogin())
SAVE_DIR = path.join(HOME_DIR, "data", "movieratings")
DATA_DIR = path.join("/home", "students", "data", "ml-100k")

MOVIE_DATA_FILE = path.join(DATA_DIR, "u.item")
GEN_Y_FILE = path.join(DATA_DIR, "u.user")
RATINGS_FILE = path.join(DATA_DIR, "u.data")

print("Home Directory: {}".format(HOME_DIR))
print("Save Directory: {}".format(SAVE_DIR))
print("Data Directory: {}\n".format(DATA_DIR))

print("Movie Data File: {}".format(MOVIE_DATA_FILE))
print("Gen Y File: {}".format(GEN_Y_FILE))
print("Ratings File: {}".format(RATINGS_FILE))

MOVIE_FIELDS = ['movie_id', 'title']
USER_FIELDS = ['user_id', 'age', 'gender']
RATING_FIELDS = ['user_id', 'movie_id', 'rating', 'timestamp']

Movie = namedtuple('Movie', MOVIE_FIELDS)
User = namedtuple('User', USER_FIELDS)
Rating = namedtuple('Rating', RATING_FIELDS)


# In[ ]:


# Custom parsing methods
def parse_movie(row):
    """
    Parses a movie row and returns a Movie named tuple.
    """
    return Movie(*row[:2])

def parse_user(row):
    """
    Parses a user row and returns a User named tuple.
    """
    row[1] = int(row[1])  # convert age to int
    return User(*row[:3])

def parse_rating(row):
    """
    Parses a rating row and returns a Rating named tuple.
    """
    row[2] = float(row[2])  # convert rating to float
    return Rating(*row[:4])


# In[ ]:


# Create a Spark context to use
sc = SparkContext(appName="Movie Rating Analysis")


# In[ ]:


# Create movies RDD
movies = sc.textFile(MOVIE_DATA_FILE).map(lambda x: x.split('|')).map(parse_movie)

for movie in movies.take(5):
    print(movie)


# In[ ]:


# Convert to pair RDD of (movie_id, title) key-values
movie_pairs = movies.map(lambda m: (m.movie_id, m.title))

# movie_id, title
for movie in movie_pairs.take(5):
    print(movie)


# In[ ]:


# Create gen_y RDD and filter on 18-24 age group, collect only user_ids
gen_y = sc.textFile(GEN_Y_FILE).map(lambda x: x.split('|')).map(parse_user).filter(lambda u: u.age >= 18 and u.age <= 24).map(lambda u: u.user_id).collect()
# user_id
print(gen_y[0])


# In[ ]:


# Broadcast gen_y to cache lookup
gen_y_ids = sc.broadcast(set(gen_y))


# In[ ]:


# Create ratings RDD
ratings = sc.textFile(RATINGS_FILE).map(lambda x: x.split('\t')).map(parse_rating)

# user_id, movie_id, rating, timestamp
for rating in ratings.take(5):
    print(rating)


# In[ ]:


# Filter ratings on gen_y users
gen_y_ratings = ratings.filter(lambda r: r.user_id in gen_y_ids.value)

# user_id, movie_id, rating, timestamp
for rating in gen_y_ratings.take(5):
    print(rating)


# In[ ]:


# Convert ratings to a pair RDD of (movie_id, rating)
rating_pairs = gen_y_ratings.map(lambda r: (r.movie_id, r.rating))

# movie_id, rating
for rating in rating_pairs.take(5):
    print(rating)


# ## Compute Average Ratings

# In[ ]:


# Sum the count of ratings
rating_sum_count = rating_pairs.combineByKey(lambda value: (value, 1),
                                             lambda x, value: (x[0] + value, x[1] + 1),
                                             lambda x, y: (x[0] + y[0], x[1] + y[1]))
# (movie_id, (rating_sum, rating_count))
for rating in rating_sum_count.take(5):
    print(rating)


# In[ ]:


# Create a new RDD with better formatted tuples
rsc_flat = rating_sum_count.map(lambda x : (x[0], x[1][0], x[1][1]))
for rating in rsc_flat.take(5):
    print(rating)


# In[ ]:


# Create an RDD with the movie_id and the average rating
rating_avg = rsc_flat.map(lambda x: (x[0], x[1] / x[2]))

for rating in rating_avg.take(5):
    print(rating)


# In[ ]:


# Join movie and rating data on movie_id
movie_rating_avg = movie_pairs.join(rating_avg)

for movie_rating in movie_rating_avg.take(5):
    print(movie_rating)


# In[ ]:


# Retain title and average rating
movie_ratings = movie_rating_avg.map(lambda x: (x[0], x[1][0], x[1][1]))

# movie_id, title, average_rating
for rating in movie_ratings.take(5):
    print(rating)


# In[ ]:


# Sort the ratings
sorted_ratings = movie_ratings.sortBy(lambda x: x[2])


# In[ ]:


# Save the results to a file
sorted_ratings.coalesce(1).saveAsTextFile(SAVE_DIR)


# In[ ]:


# Shut it down
sc.stop()

