from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
import sys
import requests
from operator import add
import json
import re
from collections import namedtuple

# Twitter breaks news quicker than news channels. Example, when UR airways flight crashed on NYC's hudson river, Twitter 
# broke the news before traditional media. This underscores Twitter's role in breaking news.
# So this task is to find the most useful and reliable source of breaking news. List the top 5 users with most retweets in the last 5 minutes.
# Whoever has the most re-tweets is the most reliable. Google's idea is that the page that has the most references wins. 
# Similarly if you are re-tweeting someone, it means you trust them and hence whoever has the most re-tweets is the most reliable.
# The text of all re-tweets begin with "RT @<user>:". This user is the original author of the tweet. So I extract the <user> parameter
# from the text and count this users occurence. 
# Then sort by most occurences. There is also a parameter called re-tweet count but I did not use that because most tweets had 0 
# re-tweets in real time. I wanted to find which user is being re-tweeted most right now. 
# The idea is I can then follow these reliable users on twitter and get breaking news from them.

def aggregate_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)
    
def extractText(tweet):
    msg = json.loads(tweet)
    news = msg['text']
    return news

def extractRetweetUser(str):
    r1 = re.search('RT @(.+): ', str)
    r2 = r1.group(0).replace('RT @','')
    i = r2.find(':')
    r3 = r2[0:i]
    return r3

def findRetweets(tweet):
    msg = json.loads(tweet)
    if msg['text'].startswith("RT @"):
        return True
    else:
        return False
    
    

# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterBreakingNewsApplication")

# create spark context with the above configuration
sc = SparkContext(conf=conf)

sc.setLogLevel("FATAL")

# create the Streaming Context from the above spark context with interval size 10 seconds
# With a batch interval of 10 seconds, the messages would accumulate for 10 seconds and then get processed.
ssc = StreamingContext(sc, 10)

# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterBreakingNewsApplication")

# read data from port 7070
dataStream = ssc.socketTextStream("localhost", 7070)

#The RDD will be created for every 10 seconds, but the data in RDD will be for the last 300 seconds
# tweets in batches of 20s each for the past 300s
lines = dataStream.window(300, 20)\
                    .filter(findRetweets)\
                    .map(extractText)\
                    .map(extractRetweetUser)\
                    .map(lambda x: (x, 1))\
                    .reduceByKey(add)

totals = lines.updateStateByKey(aggregate_count).transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))

lines.pprint(20)
# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()
