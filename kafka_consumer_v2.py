#!/usr/bin/python3
from confluent_kafka import Consumer, KafkaError, KafkaException, OFFSET_END
from datetime import datetime, timedelta
import argparse 
import json
import requests

def my_assign (consumer, partitions):
  for p in partitions:
    p.offset = OFFSET_END
  print('assign', partitions)
  consumer.assign(partitions)

def load_credentials():
    try:
        with open('credentials.json') as f:
            data = json.load(f)
            return data
    except:
        print("There was an error loading the credentials. Contact MBD Team D Section 2 for support.")
        
def add_to_playlist(message):
    message = message.value().decode('utf-8')
    
    credentials = load_credentials()
    token_type = 'Bearer'
    header = {"Authorization": '{} {}'.format(token_type,credentials['token'])}
    data = {"uris": ["spotify:track:{}".format(message)], "position": 0}
    header['Content-Type'] = 'application/json'
    playlist_id = '4JJmlxODSah5VYAoqGeOqR' # This is the offocial playlist of the master. Haven't you joined yet??
    try:
        r = requests.post('https://api.spotify.com/v1' + '/playlists/{}/tracks'.format(playlist_id), headers = header, data=json.dumps(data))
        print("ID to add: {}".format(message))
        print(r.text)
    except Exception as e:
        print(e)

parser = argparse.ArgumentParser()
parser.add_argument("topic_name", help="name of the topic to consume from")
parser.add_argument("group_id", help="group identifier this consumer belongs to")
parser.add_argument("secs", type=int, help="number of seconds reading from the topic")

args = parser.parse_args()

dt_start = datetime.now()
# Consumer setup
#
conf = {'bootstrap.servers': "localhost:9092",
        'auto.offset.reset': 'latest',
        'group.id': args.group_id}
consumer = Consumer(conf)

# Consumer subscription and message processing
#
try:
  consumer.subscribe([args.topic_name], on_assign=my_assign)

  while (dt_start + timedelta(seconds=args.secs))>datetime.now():
    message = consumer.poll(timeout=1.0)
    if message is None: continue
    print ("message")

    if message.error():
      if message.error().code() == KafkaError._PARTITION_EOF:
        # End of partition event
        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                         (message.topic(), message.partition(), 
                          message.offset()))
      elif message.error():
        raise KafkaException(message.error())
    else:
      add_to_playlist(message)
finally:
  # Close down consumer to commit final offsets.
  consumer.close()
