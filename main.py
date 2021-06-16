#!/usr/bin/env python3

import datetime
import json
import os
import random
import ssl
import subprocess
import sys
from contextlib import closing
from hashlib import md5
from tempfile import gettempdir

import boto3
import paho.mqtt.client as mqtt
import redis
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

# config
load_dotenv()

# mqtt
mqtt_broker = os.getenv('MQTT_HOST')
mqtt_port = int(os.getenv('MQTT_PORT'))
mqtt_topic = os.getenv('MQTT_TOPIC')
mqtt_client_id = f'python-mqtt-{random.randint(0, 1000)}'
mqtt_username = os.getenv('MQTT_USERNAME')
mqtt_password = os.getenv('MQTT_PASSWORD')
mqtt_ca_path = os.getenv('MQTT_CA_PATH')

# Polly
voiceId = os.getenv('POLLY_VOICE_ID')
engine = os.getenv('POLLY_ENGINE')
outputFormat = os.getenv('POLLY_OUTPUT_FORMAT')

LOG_LEVELS = {
    1: 'ERROR',
    2: 'INFO ',
    3: 'DEBUG',
}

LOG_ERROR = 1
LOG_INFO = 2
LOG_DEBUG = 3

LOG_LEVEL = int(os.getenv('LOG_LEVEL'))

REDIS_ENABLE = bool(int(os.getenv('REDIS_ENABLE')))


def log(level, msg):
    if level <= LOG_LEVEL:
        formatted_timestamp = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        print(formatted_timestamp + ' [' + LOG_LEVELS[level] + ']: ' + msg)


def on_mqtt_log(client, userdata, level, buf):
    log(LOG_DEBUG, buf)


def on_mqtt_connect(client, userdata, flags, rc):
    log(LOG_INFO, 'MQTT: Connected.')


def on_mqtt_message(client, userdata, message):
    payload = message.payload.decode("utf-8")
    log(LOG_DEBUG, "MQTT-in: topic=" + str(message.topic) + ' qos=' + str(message.qos) + ' retain=' +
        str(message.retain) + ' payload=' + str(payload))

    json_payload = json.loads(payload)
    text_to_speech(json_payload)


def get_mqtt_client():
    client = mqtt.Client()
    client.on_message = on_mqtt_message
    client.on_log = on_mqtt_log
    client.on_connect = on_mqtt_connect
    client.tls_set(mqtt_ca_path,
                   tls_version=ssl.PROTOCOL_TLSv1_2)
    client.tls_insecure_set(True)
    client.username_pw_set(mqtt_username, mqtt_password)
    log(LOG_INFO, "connecting to broker " + mqtt_broker)
    client.connect(mqtt_broker, mqtt_port)

    # wait to allow publish and logging and exit

    timestamp = datetime.datetime.now()
    msg = json.dumps({
        'state': True,
        'ts': timestamp.timestamp(),
        'data': 'heyho'
    })

#    client.publish(mqtt_topic, msg)
    client.subscribe(mqtt_topic)

    return client


def get_redis_client():
    return redis.Redis()


def get_from_redis(hash_value):
    r = get_redis_client()
    key = 'mp3_' + hash_value

    if r.exists(key):
        log(LOG_INFO, 'Redis: Get key ' + key)
        return r.get(key)
    else:
        log(LOG_INFO, 'Redis: No such key ' + key)
    return None


def add_to_redis(hash_value, text, content):
    r = get_redis_client()

    mp3_key = 'mp3_' + hash_value
    r.set(mp3_key, content)

    text_key = 'text_' + hash_value
    r.set(text_key, text)

    log(LOG_INFO, 'Redis: Added sound stream with key ' + mp3_key + ' for text "' + text + '"')


def text_to_speech(message):
    text = str(message['text'])
    text_hash = md5(text.encode()).hexdigest()
    output = os.path.join(gettempdir(), "speech.mp3")

    content = None
    success = False

    if REDIS_ENABLE:
        content = get_from_redis(text_hash)

    if content:
        log(LOG_INFO, "Found cached sound file in Redis")
        with open(output, "wb") as file:
            file.write(content)
        success = True
    else:
        content = get_from_polly(text)

        if content:
            log(LOG_INFO, "Generated new sound file with Polly")
            with open(output, "wb") as file:
                file.write(content)

            if REDIS_ENABLE:
                add_to_redis(text_hash, text, content)

            success = True

    if success:
        play_file(output)


def get_from_polly(message):
    session = boto3.Session(
        aws_access_key_id=os.getenv('AWS_ACCESS_ID'),
        aws_secret_access_key=os.getenv('AWS_ACCESS_SECRET'),
        region_name=os.getenv('AWS_REGION'))

    polly = session.client("polly")
    response = None

    try:
        log(LOG_INFO, 'Requesting Polly speech synthesis')
        ssml_text = '<speak>' + message + '</speak>'

        response = polly.synthesize_speech(OutputFormat=outputFormat, VoiceId=voiceId, Engine=engine,
                   TextType='ssml',
                   Text=str(ssml_text))
    except (BotoCoreError, ClientError) as error:
        log(LOG_ERROR, error)

    # Access the audio stream from the response
    if "AudioStream" in response:
        # Note: Closing the stream is important because the service throttles on the
        # number of parallel connections. Here we are using contextlib.closing to
        # ensure the close method of the stream object will be called automatically
        # at the end of the with statement's scope.

        try:
            with closing(response['AudioStream']) as stream:
                return stream.read()
        except IOError as error:
            log(LOG_ERROR, error)
    else:
        # The response didn't contain audio data, exit gracefully
        log(LOG_ERROR, "Could not retrieve stream from Polly")


def play_file(path):
    log(LOG_INFO, 'Play sound file ' + path)
    # Play the audio using the platform's default player
    if sys.platform == "win32":
        os.startfile(path)
    else:
        # The following works on macOS and Linux. (Darwin = mac, xdg-open = linux).
        opener = "open" if sys.platform == "darwin" else "xdg-open"
        subprocess.call([opener, path])


mqtt_client = get_mqtt_client()

try:
    mqtt_client.loop_forever()
except KeyboardInterrupt:
    log(LOG_INFO, "Exiting.")
finally:
    mqtt_client.loop_stop()
    log(LOG_INFO, "End.")
