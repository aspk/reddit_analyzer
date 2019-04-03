# kafka consumer
from kafka import KafkaConsumer

consumer = KafkaConsumer('reddit-stream-topic',
                         group_id='spark',
                         bootstrap_servers=['10.0.0.5:9092'],
                         auto_offset_reset = 'latest')
for message in consumer:
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))
