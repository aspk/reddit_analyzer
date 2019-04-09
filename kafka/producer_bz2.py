from boto.s3.connection import S3Connection
import datetime
import json
import bz2
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time
import pytz

conn = S3Connection()
key = conn.get_bucket('aspk-reddit-posts').get_key('comments/RC_2017-11.bz2')

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
count = 0
decomp = bz2.BZ2Decompressor()


CHUNK_SIZE= 5000*1024
timezone = pytz.timezone("America/Los_Angeles")
start_time = time.time()
while True:
    print('in')
    chunk = key.read(CHUNK_SIZE)
    if not chunk:
        break
    data = decomp.decompress(chunk).decode()


    for i in data.split('\n'):
            count+=1
            if count%100000==0 and count!=0:
                print('rate of kafka producer messages is {}'.format(count/(time.time()-start_time)))
            try:
                comment = json.loads(i)
                reddit_event = {}
                reddit_event['post'] = comment['permalink'].split('/')[-3]
                reddit_event['subreddit'] = comment['subreddit']
                reddit_event['timestamp'] = str(datetime.datetime.fromtimestamp(time.time()))
                #d = datetime.datetime.now()
                #d_aware = timezone.localize(d)
                #reddit_event['timestamp'] = str(d_aware)
                reddit_event['body'] = comment['body']
                reddit_event['author'] = comment['author']
                # print('dict')
                # print(reddit_event)
                # print('dumps')
                # print(bytes(json.dumps(reddit_event),'utf-8'))
                producer.send('reddit-stream-topic', bytes(json.dumps(reddit_event),'utf-8'))
                producer.flush()
                time.sleep(0.001)
                #print(2)
            except:
                #print(1)
                #print(1)
                flag = 0
                oldi = i


    #break
