import time
import socket
import random


def generate_tweet():
    choice = random.choice(["python", "data", "coding", "engineer"])
    return f'A #{choice} B'


if __name__ == "__main__":
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    host = '127.0.0.1'
    port = 5555
    s.bind((host, port))
    print(f'Listening on port {port}')

    s.listen(3)
    client, _ = s.accept()
    print(f"Received request from " + str(_))
    while True:
        tweet_data = generate_tweet()
        print(f'Sending message: {tweet_data}')
        client.sendall(tweet_data.encode('utf-8'))
        time.sleep(random.uniform(0.5, 2.0))
