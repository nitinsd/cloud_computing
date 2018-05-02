from __future__ import absolute_import, print_function

import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

import socket
import json

# Go to http://apps.twitter.com and create an app.
# The consumer key and secret will be generated for you after
consumer_key=""
consumer_secret=""

# After the step above, you will be redirected to your app's page.
# Create an access token under the the "Your access token" section
access_token=""
access_token_secret=""

class TweetsListener(StreamListener):
  def __init__(self, csocket):
      self.client_socket = csocket
  def on_data(self, data):
      self.client_socket.send(data)
  def on_error(self, status):
      print(status)
      return True

def sendData(c_socket):
  auth = OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_token_secret)
  twitter_stream = Stream(auth, TweetsListener(c_socket))
  twitter_stream.filter(languages=["en"], track=['data'])

s = socket.socket()
TCP_IP = "localhost"
TCP_PORT = 7070

s.bind((TCP_IP, TCP_PORT))
s.listen(1)

print("Wait here for TCP connection ...")

conn, addr = s.accept()

print("Connected, lets go get tweets.")
sendData(conn)
