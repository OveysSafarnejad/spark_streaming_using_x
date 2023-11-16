import json
import os
import socket

from dotenv import load_dotenv
from tweepy import Stream
from tweepy.auth import OAuthHandler
from tweepy.streaming import StreamListener

load_dotenv()

CUNSUMER_KEY = os.getenv("CUNSUMER_KEY")
CUNSUMER_SECRET = os.getenv("CUNSUMER_SECRET")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
ACCESS_SECRET = os.getenv("ACCESS_SECRET")


class TweetsListener(StreamListener):

    def __init__(self, csocket):
        self.client_socket = csocket
        super().__init__()

    def on_data(self, data):
        try:
            msg = json.loads(data)
            print(msg['text'].encode('utf-8'))
            self.client_socket.send(msg['text'].encode('utf-8'))
            return True
        except BaseException as e:
            print('Error on data %s' % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


def send_data(c_socket):
    print("AUTH DATA IS: ")
    print(CUNSUMER_KEY)
    print(CUNSUMER_SECRET)

    auth = OAuthHandler(CUNSUMER_KEY, CUNSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

    tweeter_stream = Stream(auth=auth, listener=TweetsListener(c_socket))
    tweeter_stream.filter(track=['football'])


if __name__ == "__main__":
    s = socket.socket()
    host = '127.0.0.1'
    port = 5555
    s.bind((host, port))
    print(f'Listening on port {port}')

    s.listen(5)
    c, addr = s.accept()
    print(f"Received request from " + str(addr))
    send_data(c)
