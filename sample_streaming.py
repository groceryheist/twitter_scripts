import json
import tweepy
from queue import SimpleQueue, Full
from math import ceil
from threading import Thread
from os import path
from http.client import IncompleteRead
from urllib3.exceptions import ProtocolError
# input your own authentication info
apps = []
api_keys = []
api_secrets = []
access_tokens = []
access_token_secrets = []

#keywords=["Logan","Rams","Paul","49ers","santa","trump","jenner"]

# set your keywords
keywords=[]

# we use a simple queue so we can stream on multiple threads to use multiple apps. 
status_queue = SimpleQueue()

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

def authenticate(api_key, api_secret, access_token, access_token_secret):
    auth = tweepy.OAuthHandler(api_key, api_secret)
    auth.set_access_token(access_token, access_token_secret)

    return auth

class OutputThread(Thread):
    def __init__(self, queue, outfile):
        super().__init__()
        self.queue = queue
        self.outfile = outfile
        self.open_file()

    def open_file(self):
        if path.exists(self.outfile):
            self.o = open(self.outfile, 'w')
        else:
            self.o = open(self.outfile, 'a')

    def write_line(self, line):
        if self.o.closed:
            self.open_file()

        self.o.write(line)

    def run(self):
        while True:
            status_obj = self.queue.get()
            line = json.dumps(status_obj._json)
            self.write_line(line + '\n')

class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        try:
            status_queue.put(status)
        except Full as e:
            print("QUEUE IS FULL! LOSING DATA!")
            return

    def on_exception(self, e):
        print(e)
        return

    def on_error(self, status_code):
        print(status_code)

        if status_code == 420:
            #returning False in on_error disconnects the stream
            return False

output_thread = OutputThread(status_queue, './test.json') 
output_thread.setDaemon(True)
output_thread.start()

streams = [tweepy.Stream(auth, MyStreamListener()) for auth in auths]

close = lambda ls: [l.disconnect() for l in ls]

n_streams = len(streams)

keyword_groups = list(chunks(keywords, ceil(len(keywords)/n_streams)))

def filter_async_with_reconnect(stream, track, stall_warnings):
    # start my own thread
    def inner_func(stream, track, stall_warnings):
        while True:
            try:
                print("starting stream")
                stream.filter(track=track, is_async=False, stall_warnings=stall_warnings)
            except (IncompleteRead, ProtocolError) as e:
                print("Incomplete Read Ignored")
                continue
            except KeyboardInterrupt:
                stream.disconnect()
                break
    
    t = Thread(target=inner_func, kwargs={'stream':stream, 'track':track, 'stall_warnings':stall_warnings})
    t.start()
    return t

filters = [filter_async_with_reconnect(stream, track=group, stall_warnings=True) for stream, group in zip(streams, keyword_groups)]

output_thread.join()
