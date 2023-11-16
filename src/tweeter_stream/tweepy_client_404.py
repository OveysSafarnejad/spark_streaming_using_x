import os
import json
import socket

import tweepy
from dotenv import load_dotenv

load_dotenv()

consumer_key = os.getenv("CUNSUMER_KEY")
consumer_secret = os.getenv("CUNSUMER_SECRET")
access_token = os.getenv("ACCESS_TOKEN")
access_token_secret = os.getenv("ACCESS_SECRET")

# Authenticate to Twitter
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

# Create an API object
api = tweepy.API(auth)


class MyStreamListener(tweepy.StreamListener):
    def __init__(self, csocket):
        super().__init__()
        self.csocket = csocket

    def on_error(self, status):
        print(status)
        return True

    def on_status(self, status):
        print(status.text)

    def on_data(self, raw_data):
        try:
            msg = json.loads(raw_data)
            print(msg['text'].encode('utf-8'))
            self.csocket.send(msg['text'].encode('utf-8'))
            return True
        except BaseException as e:
            print('Error on data %s' % str(e))
        return True


if __name__ == "__main__":
    s = socket.socket()
    host = '127.0.0.1'
    port = 5555
    s.bind((host, port))
    print(f'Listening on port {port}')

    s.listen(5)
    c, addr = s.accept()
    print(f"Received request from " + str(addr))

    my_stream_listener = MyStreamListener(csocket=c)

    # Create a streaming API connection
    my_stream = tweepy.Stream(auth=api.auth, listener=my_stream_listener)

    # Set up a filter for tweets containing a specific keyword
    my_stream.filter(track=['python'])

