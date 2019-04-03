# kafka producer
from kafka import KafkaProducer
from kafka.errors import KafkaError
from sseclient import SSEClient
import json
import datetime
import time
import bz2

#from ftfy import fix_text



producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

"""
kafka producer
json format author, body, timestamp, post and subreddit
"""

source_file = bz2.BZ2File('/Volumes/akeasystore/RC_2017-11.bz2', "r")
for line in source_file:
    comment = json.loads(line)
    reddit_event = {}
    reddit_event['post'] = comment['permalink'].split('/')[-3]
    reddit_event['subreddit'] = comment['subreddit']
    reddit_event['timestamp'] = str(datetime.datetime.fromtimestamp(time.time()))
    reddit_event['body'] = comment['body']
    reddit_event['author'] = comment['author']
    producer.send('reddit-stream-topic', bytes(json.dumps(reddit_event),'utf-8'))
    producer.flush()
    time.sleep(0.1)

source_file.close()
