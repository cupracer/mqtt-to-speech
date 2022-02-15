#!/usr/bin/env python3

import argparse
import json
import logging
import os
import random
import ssl
from contextlib import closing
from hashlib import md5

import boto3
import paho.mqtt.client as mqtt
import redis
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

from player import Player
from utils import get_project_root


class MqttToSpeech:
    # MQTT
    mqtt_broker = None
    mqtt_port = None
    mqtt_topic = None
    mqtt_client_id = None
    mqtt_username = None
    mqtt_password = None
    mqtt_ca_path = None

    # Amazon Polly
    voiceId = None
    engine = None
    outputFormat = None

    # Redis
    use_redis = False
    redis_session = None

    message_prefix_sound_file = None
    message_prefix_sound = None
    message_default_volume = 0.8

    runner = None
    player = None

    def __init__(self, *args, **kwargs):
        # args -- tuple of anonymous arguments
        # kwargs -- dictionary of named arguments

        log_level = str(kwargs.get('log_level', 'DEBUG'))

        if bool(kwargs.get('standalone', False)):
            logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=log_level.upper())
        else:
            logging.basicConfig(format='%(levelname)s: %(message)s', level=log_level.upper())

        self.mqtt_broker = str(kwargs.get('mqtt_broker'))
        self.mqtt_port = int(kwargs.get('mqtt_port', 8883))
        self.mqtt_topic = str(kwargs.get('mqtt_topic'))
        self.mqtt_username = str(kwargs.get('mqtt_username'))
        self.mqtt_password = str(kwargs.get('mqtt_password'))
        self.mqtt_ca_path = str(kwargs.get('mqtt_ca_path'))
        self.mqtt_client_id = f'python-mqtt-{random.randint(0, 1000)}'

        self.voiceId = str(kwargs.get('polly_voice_id', 'Vicki'))
        self.engine = str(kwargs.get('polly_engine', 'neural'))
        self.outputFormat = str(kwargs.get('polly_output_format', 'mp3'))

        self.use_redis = bool(int(kwargs.get('use_redis', 0)))
        self.message_prefix_sound_file = kwargs.get('message_prefix_sound_file', None)

    def on_mqtt_log(self, client, userdata, level, buf):
        logging.debug(buf)

    def on_mqtt_connect(self, client, userdata, flags, rc):
        logging.info('MQTT: Connected.')
        logging.info("MQTT: Subscribing topic " + self.mqtt_topic)
        client.subscribe(self.mqtt_topic)

    def on_mqtt_disconnect(self, client, userdata, rc):
        logging.info('MQTT: Disconnected.')

    def on_mqtt_message(self, client, userdata, message):
        payload = message.payload.decode("utf-8")
        logging.debug("MQTT-in: topic=" + str(message.topic) + ' qos=' + str(message.qos) + ' retain=' +
                 str(message.retain) + ' payload=' + str(payload))

        try:
            json_payload = json.loads(payload)
            self.text_to_speech(json_payload)
        except Exception as err:
            logging.debug('on_mqtt_message: An error occurred: ' + str(err))

    def get_mqtt_client(self):
        client = mqtt.Client()
        client.on_message = self.on_mqtt_message
        client.on_log = self.on_mqtt_log
        client.on_connect = self.on_mqtt_connect
        client.on_disconnect = self.on_mqtt_disconnect
        client.tls_set(self.mqtt_ca_path,
                       tls_version=ssl.PROTOCOL_TLSv1_2)
        client.tls_insecure_set(True)
        client.username_pw_set(self.mqtt_username, self.mqtt_password)

        logging.info("connecting to broker " + self.mqtt_broker)
        client.connect(self.mqtt_broker, self.mqtt_port)

        return client

    def get_redis_session(self):
        if not self.redis_session or not self.redis_session.ping():
            logging.debug('Init new Redis connection')
            self.redis_session = redis.Redis()
        else:
            logging.debug('Use existing Redis connection')

        return self.redis_session

    def get_from_redis(self, hash_value):
        r = self.get_redis_session()
        key = 'mp3_' + hash_value

        if r.exists(key):
            logging.debug('Redis: Get key ' + key)
            return r.get(key)
        else:
            logging.debug('Redis: No such key ' + key)
        return None

    def add_to_redis(self, hash_value, text, content):
        r = self.get_redis_session()

        mp3_key = 'mp3_' + hash_value
        r.set(mp3_key, content)

        text_key = 'text_' + hash_value
        r.set(text_key, text)

        logging.info('Redis: Added sound stream with key ' + mp3_key + ' for text "' + text + '"')

    def get_message_prefix_sound_buffer(self):
        if self.message_prefix_sound_file is None:
            logging.debug('Prefix sound file not set.')
            return None

        if not self.message_prefix_sound:
            try:
                with open(os.path.join(get_project_root(), self.message_prefix_sound_file), 'rb') as infile:
                    self.message_prefix_sound = infile.read()
            except FileNotFoundError as err:
                logging.error('Could not read prefix sound file: %s', err)
                return None

        return self.message_prefix_sound

    def text_to_speech(self, message):
        text = str(message['text'])
        text_hash = md5(text.encode()).hexdigest()
        volume_percent = 100

        if 'volume' in message:
            volume_percent = int(message['volume'])

        content_buffer = None
        success = False

        if self.use_redis:
            content_buffer = self.get_from_redis(text_hash)

        if content_buffer:
            logging.info("Found cached sound stream in Redis")
            success = True
        else:
            content_buffer = self.get_from_polly(text)

            if content_buffer:
                logging.info("Generated new sound stream with Polly")
                if self.use_redis:
                    self.add_to_redis(text_hash, text, content_buffer)

                success = True

        if success:
            prefix_sound = self.get_message_prefix_sound_buffer()

            if not self.player or not self.player.is_alive():
                if prefix_sound:
                    logging.info("Play sound stream with prefix")
                    self.player = Player(
                        message_buffer=content_buffer,
                        prefix_buffer=prefix_sound,
                        volume_percent=volume_percent
                    )
                else:
                    logging.info("Play sound stream without prefix")
                    self.player = Player(
                        message_buffer=content_buffer,
                        volume_percent=volume_percent
                    )
            else:
                self.player.join()
                logging.info("Play sound stream without prefix")
                self.player = Player(
                    message_buffer=content_buffer,
                    volume_percent=volume_percent
                )

            self.player.start()

    def get_from_polly(self, message):
        session = boto3.Session(
            aws_access_key_id=os.getenv('AWS_ACCESS_ID'),
            aws_secret_access_key=os.getenv('AWS_ACCESS_SECRET'),
            region_name=os.getenv('AWS_REGION'))

        polly = session.client("polly")
        response = None

        try:
            logging.info('Requesting Polly speech synthesis')
            ssml_text = '<speak>' + message + '</speak>'

            response = polly.synthesize_speech(OutputFormat=self.outputFormat,
                                               VoiceId=self.voiceId,
                                               Engine=self.engine,
                                               TextType='ssml',
                                               Text=ssml_text)
        except (BotoCoreError, ClientError) as error:
            logging.error(error)

        # Access the audio stream from the response
        if "AudioStream" in response:
            logging.debug("Found AudioStream in Polly response")

            # Note: Closing the stream is important because the service throttles on the
            # number of parallel connections. Here we are using contextlib.closing to
            # ensure the close method of the stream object will be called automatically
            # at the end of the with statement's scope.
            try:
                with closing(response['AudioStream']) as polly_stream:
                    return polly_stream.read()
            except Exception as error:
                logging.error(error)
        else:
            # The response didn't contain audio data, exit gracefully
            logging.error("Could not retrieve stream from Polly")

    def start_runner(self):
        self.runner = self.get_mqtt_client()
        self.runner.loop_forever()

    def kill_runner(self):
        self.runner.loop_stop()


def main(standalone=True):
    load_dotenv()

    mqtt_to_speech = MqttToSpeech(
        standalone=standalone,
        mqtt_broker=os.getenv('MQTT_HOST'),
        mqtt_port=int(os.getenv('MQTT_PORT')),
        mqtt_topic=os.getenv('MQTT_TOPIC'),
        mqtt_username=os.getenv('MQTT_USERNAME'),
        mqtt_password=os.getenv('MQTT_PASSWORD'),
        mqtt_ca_path=os.getenv('MQTT_CA_PATH'),
        mqtt_client_id=f'python-mqtt-{random.randint(0, 1000)}',
        polly_voice_id=os.getenv('POLLY_VOICE_ID'),
        polly_engine=os.getenv('POLLY_ENGINE'),
        polly_output_format=os.getenv('POLLY_OUTPUT_FORMAT'),
        log_level=str(os.getenv('LOG_LEVEL')),
        use_redis=bool(int(os.getenv('USE_REDIS'))),
        message_prefix_sound_file=os.getenv('MESSAGE_PREFIX_SOUND_FILE'),
    )

    try:
        mqtt_to_speech.start_runner()
    except KeyboardInterrupt:
        logging.info("Exiting.")
        mqtt_to_speech.kill_runner()
    finally:
        logging.info("End.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # Python>=3.9 required:
    #parser.add_argument('--standalone', dest='standalone', action=argparse.BooleanOptionalAction, default=True,
    #                    help='print/omit timestamps in log messages')

    parser.add_argument('--no-standalone', dest='standalone', action='store_false',
                        help='omit timestamps in log messages')
    parser.set_defaults(standalone=True)

    args = parser.parse_args()
    main(args.standalone)
