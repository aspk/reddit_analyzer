# kafka consumer
from kafka import KafkaConsumer
from postgres_functions import postgres_insert,postgres_delete
import json

consumer = KafkaConsumer('reddit-spark-topic',
                         group_id='spark',
                         bootstrap_servers=['10.0.0.5:9092'],
                         auto_offset_reset = 'latest')
flag = 0
for message in consumer:
    # print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
    #                                       message.offset, message.key,
    #                                       message.value))
    #print(message.value)
    spark_output = json.loads(message.value)
    #example {"post":"devils_practice_lines_for_today","window":{"start":"2019-04-02T23:20:08.000Z","end":"2019-04-02T23:20:18.000Z"},"count":1}
    if flag ==0:
        postgres_insert(msg)
        flag =1
        s = msg['window']['end'].replace('T',' ').replace('Z','')[:-4]
        timestamp = time.mktime(datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").timetuple())
    else:
        s = msg['window']['end'].replace('T',' ').replace('Z','')[:-4]
        new_timestamp = time.mktime(datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").timetuple())
        if new_timestamp>timestamp:
            postgres_delete(msg)
            postgres_insert(msg)
        else:
            postgres_insert(msg)
