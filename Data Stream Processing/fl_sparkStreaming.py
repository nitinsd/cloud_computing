from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
import sys
import requests
from operator import add
import json

def aggregate_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

def makefollowerSplit(line):
    msg = json.loads(line)
    tweet_text=msg['text']
    user_name= str(msg['user']['screen_name']).encode('utf-8')
    value = user_name + " --- "+ tweet_text 
    # print(value)
    return (msg['user']['followers_count'], value)

# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApplication")

# create spark context with the above configuration
sc = SparkContext(conf=conf)

# sc.setLogLevel("INFO")
# sc.setLogLevel("ERROR")
sc.setLogLevel("FATAL")

# create the Streaming Context from the above spark context with interval size 2 seconds
ssc = StreamingContext(sc, 5)

# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 7070
dataStream = ssc.socketTextStream("localhost", 7070)


# split each tweet into words
wDStream = dataStream.window(120, 10)
words = wDStream.map(makefollowerSplit)

# hashtags = words.filter(lambda w: ('#' in w))\
#                 .filter(lambda w: not (('http' in w) or ('\\u' in w) or (len(w)==1) ))\
#                 .map(lambda x: (x.lower(), 1))\
#                 .reduceByKey(add)

# adding the count of each hashtag to its last count
tags_totals = words.transform(lambda rdd: rdd.sortByKey(ascending=False))

# Debuging code
tags_totals.pprint(20)

# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()
