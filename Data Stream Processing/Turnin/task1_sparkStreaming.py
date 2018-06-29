from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
import sys
import requests
from operator import add
import json

def checkForData(tweet):
    msg = json.loads(tweet)
    if "data" in msg['text'].lower():
        return True
    else:
        return False

def followers(tweet):
    msg = json.loads(tweet)
    key = msg['user']['screen_name'] + " ----- " + msg['text']
    value = msg['user']['followers_count']
    return (key, value)

# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApplication")

# create spark context with the above configuration
sc = SparkContext(conf=conf)

sc.setLogLevel("FATAL")

# create the Streaming Context from the above spark context with interval size 2 seconds
ssc = StreamingContext(sc, 2)

# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 7070
dataStream = ssc.socketTextStream("localhost", 7070)

# tweets in batches of 20s each for the past 300s
# DStream contains data for last 300 seconds
wDStream = dataStream.window(300, 20)

# filter out tweets that do not contain the word "data", doing it again even though 
# tweepy is already doing it before sending it to spark
filtered = wDStream.filter(checkForData)

# return a tuple of user and follower_count and reverse the key/value for sorting
tupled = filtered.map(followers).map(lambda (x,y): (y,x))

# sort by number of followers
sorted = tupled.transform(lambda rdd: rdd.distinct().sortByKey(ascending=False))

# reverse the key/value order for display
result = sorted.map(lambda (x,y): (y,x))

result.pprint(20)

# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()
