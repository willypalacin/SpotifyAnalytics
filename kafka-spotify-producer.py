#!/usr/bin/python3
from confluent_kafka import Producer
import argparse
import requests
import socket
import time
import os
import json


def load_credentials():
    try:
        with open('credentials.json') as f:
            data = json.load(f)
            return data
    except:
        print("There was an error loading the credentials. Contact MBD Team D Section 2 for support.")


def check_player(base_url, token):
    current_id = 0
    flag = True
    while(True):
        token_type = 'Bearer'
        header = {"Authorization": '{} {}'.format(token_type,token)}
        header['Content-Type'] = 'application/json'
        
        try:
            r = requests.get(base_url + '/me/player/currently-playing', headers = header)
            r_json = r.json()
            string_current_song = "{} | {} | {} | {}".format(r_json['item']['name'],
                                             r_json['item']['artists'][0]['name'], 
                                             r_json['timestamp'], r_json['item']['id'])
            print("Sending sensor data to topic '%s'" % args.topic)
            time.sleep(5)
            print(string_current_song)
            send_message(producer, string_current_song, args.topic)
            flag = True
            if (current_id != r_json['item']['id']):
                r = requests.get(base_url + '/audio-features/{}'.format(r_json['item']['id']), headers = header)
                topic_features = '{}_features'.format(args.topic)
                print("Sending sensor data to topic '%s'" % topic_features)
                print(r.text)
                send_message(producer, r.text, '{}'.format(topic_features))
                current_id = r_json['item']['id']
        except Exception as e:
            if flag:
                print("No song being reproduced")
                flag = False
            
            
                

def send_message(producer, message, topic):
  # Checking that message and topic were specified
  #
  if message == 'None':
    print("ERROR: A message has to be provided")
    return
  if topic == 'None':
    print("ERROR: A topic has to be provided")
    return

  producer.produce(topic, value=message)
  producer.flush()

parser = argparse.ArgumentParser()
parser.add_argument("topic", help="topic where the sensor data has to be sent")
args = parser.parse_args()

# Producer setup
#
conf = {'bootstrap.servers': "localhost:9092",
        'client.id': socket.gethostname()}
producer = Producer(conf)

base_url = 'https://api.spotify.com/v1'
token = load_credentials()['token']
spotify_entries = check_player(base_url, token) 

