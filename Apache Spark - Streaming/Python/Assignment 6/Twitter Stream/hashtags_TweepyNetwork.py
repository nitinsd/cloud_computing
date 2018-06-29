from __future__ import absolute_import, print_function

import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

import socket
import json

# Go to http://apps.twitter.com and create an app.
# The consumer key and secret will be generated for you after
consumer_key="JL04nWpU3YG4NDH85t6nzQKxM"
consumer_secret="WUlWUpPfCPObv45naXhLqjGHkyspWsdbtqhjv4joV9KmquKoY3"

# After the step above, you will be redirected to your app's page.
# Create an access token under the the "Your access token" section
access_token="141400849-NfCF93hufJdiw0lYLv95BWpgXvx0DqxKp92uGwC3"
access_token_secret="v0iYsV6SbrGMCZBoE2u6Nrbd3WxYRfmDcsztAkCsywyYL"

class TweetsListener(StreamListener):
  def __init__(self, csocket):
      self.client_socket = csocket
  def on_data(self, data):
      try:
          msg = json.loads(data)
          tweet_text=msg['text'].encode('utf-8')
          print (msg['user']['screen_name'].encode('utf-8') + " --- "+ msg['text'].encode('utf-8'))
          self.client_socket.send(tweet_text )
          return True
      except BaseException as e:
          print("Error on_data: %s" % str(e))
      return True
  def on_error(self, status):
      print(status)
      return True
  
  
def sendData(c_socket):
  auth = OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_token_secret)
  twitter_stream = Stream(auth, TweetsListener(c_socket))
  twitter_stream.filter(track=['data'])

# Additional fields, like only en tweets, in us 
# twitter_stream.filter(track=['data'], languages=['en'], locations=[-130,-20,100,50])
  
  
  
s = socket.socket()
TCP_IP = "localhost"
TCP_PORT = 9009

s.bind((TCP_IP, TCP_PORT))
s.listen(1)

print("Wait here for TCP connection ...")

conn, addr = s.accept()

print("Connected, lets go get tweets.")
sendData(conn)    