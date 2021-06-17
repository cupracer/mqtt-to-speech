#!/usr/bin/env python3

import datetime
import json
import os
import random
import ssl
import io
import time

import pygame
from contextlib import closing
from hashlib import md5

import boto3
import paho.mqtt.client as mqtt
import redis
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv


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

    # Logging
    _LOG_ERROR = 1
    _LOG_INFO = 2
    _LOG_DEBUG = 3
    log_level = 2

    # Redis
    use_redis = False

    runner = None

    def _log(self, level, msg):
        log_levels = {
            1: 'ERROR',
            2: 'INFO ',
            3: 'DEBUG',
        }

        if level <= self.log_level:
            formatted_timestamp = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
            print(formatted_timestamp + ' [' + log_levels[level] + ']: ' + msg)

    def log_info(self, msg):
        self._log(self._LOG_INFO, msg)

    def log_error(self, msg):
        self._log(self._LOG_ERROR, msg)

    def log_debug(self, msg):
        self._log(self._LOG_DEBUG, msg)

    def on_mqtt_log(self, client, userdata, level, buf):
        self._log(self._LOG_DEBUG, buf)

    def on_mqtt_connect(self, client, userdata, flags, rc):
        self.log_info('MQTT: Connected.')

    def on_mqtt_message(self, client, userdata, message):
        payload = message.payload.decode("utf-8")
        self.log_debug("MQTT-in: topic=" + str(message.topic) + ' qos=' + str(message.qos) + ' retain=' +
                 str(message.retain) + ' payload=' + str(payload))

        try:
            json_payload = json.loads(payload)
            self.text_to_speech(json_payload)
        except Exception as err:
            self.log_debug('on_mqtt_message: An error occurred: ' + str(err))

    def get_mqtt_client(self):
        client = mqtt.Client()
        client.on_message = self.on_mqtt_message
        client.on_log = self.on_mqtt_log
        client.on_connect = self.on_mqtt_connect
        client.tls_set(self.mqtt_ca_path,
                       tls_version=ssl.PROTOCOL_TLSv1_2)
        client.tls_insecure_set(True)
        client.username_pw_set(self.mqtt_username, self.mqtt_password)

        self.log_info("connecting to broker " + self.mqtt_broker)
        client.connect(self.mqtt_broker, self.mqtt_port)
        self.log_info("subscribing topic " + self.mqtt_topic)
        client.subscribe(self.mqtt_topic)

        return client

    def get_from_redis(self, hash_value):
        r = redis.Redis()
        key = 'mp3_' + hash_value

        if r.exists(key):
            self.log_debug('Redis: Get key ' + key)
            return r.get(key)
        else:
            self.log_debug('Redis: No such key ' + key)
        return None

    def add_to_redis(self, hash_value, text, content):
        r = redis.Redis()

        mp3_key = 'mp3_' + hash_value
        r.set(mp3_key, content)

        text_key = 'text_' + hash_value
        r.set(text_key, text)

        self.log_info('Redis: Added sound stream with key ' + mp3_key + ' for text "' + text + '"')

    def text_to_speech(self, message):
        text = str(message['text'])
        text_hash = md5(text.encode()).hexdigest()

        content_buffer = None
        success = False

        if self.use_redis:
            content_buffer = self.get_from_redis(text_hash)

        if content_buffer:
            self.log_info("Found cached sound stream in Redis")
            success = True
        else:
            content_buffer = self.get_from_polly(text)

            if content_buffer:
                self.log_info("Generated new sound stream with Polly")
                if self.use_redis:
                    self.add_to_redis(text_hash, text, content_buffer)

                success = True

        if success:
            self.play_stream(content_buffer)

    def get_from_polly(self, message):
        session = boto3.Session(
            aws_access_key_id=os.getenv('AWS_ACCESS_ID'),
            aws_secret_access_key=os.getenv('AWS_ACCESS_SECRET'),
            region_name=os.getenv('AWS_REGION'))

        polly = session.client("polly")
        response = None

        try:
            self.log_info('Requesting Polly speech synthesis')
            ssml_text = '<speak>' + message + '</speak>'

            response = polly.synthesize_speech(OutputFormat=self.outputFormat,
                                               VoiceId=self.voiceId,
                                               Engine=self.engine,
                                               TextType='ssml',
                                               Text=ssml_text)
        except (BotoCoreError, ClientError) as error:
            self.log_error(error)

        # Access the audio stream from the response
        if "AudioStream" in response:
            self.log_debug("Found AudioStream in Polly response")

            # Note: Closing the stream is important because the service throttles on the
            # number of parallel connections. Here we are using contextlib.closing to
            # ensure the close method of the stream object will be called automatically
            # at the end of the with statement's scope.
            try:
                with closing(response['AudioStream']) as polly_stream:
                    return polly_stream.read()
            except Exception as error:
                self.log_error(error)
        else:
            # The response didn't contain audio data, exit gracefully
            self.log_error("Could not retrieve stream from Polly")

    def play_stream(self, buffer):
        self.log_info('Play sound stream')

        try:
            mixer = pygame.mixer
            mixer.init()

            self.log_debug("Load stream")
            mixer.music.load(io.BytesIO(buffer))

            self.log_debug("Play stream")
            mixer.music.play()

            while pygame.mixer.music.get_busy():
                time.sleep(0.5)

            mixer.music.unload()
            self.log_debug("Finished playing.")
        except Exception as err:
            self.log_error('Failed to play sound stream: ' + str(err))

    def start_runner(self):
        self.runner = self.get_mqtt_client()
        self.runner.loop_forever()

    def kill_runner(self):
        self.runner.loop_stop()


def main():
    load_dotenv()

    mqtt_to_speech = MqttToSpeech()

    mqtt_to_speech.mqtt_broker = os.getenv('MQTT_HOST')
    mqtt_to_speech.mqtt_port = int(os.getenv('MQTT_PORT'))
    mqtt_to_speech.mqtt_topic = os.getenv('MQTT_TOPIC')
    mqtt_to_speech.mqtt_username = os.getenv('MQTT_USERNAME')
    mqtt_to_speech.mqtt_password = os.getenv('MQTT_PASSWORD')
    mqtt_to_speech.mqtt_ca_path = os.getenv('MQTT_CA_PATH')
    mqtt_to_speech.mqtt_client_id = f'python-mqtt-{random.randint(0, 1000)}'

    mqtt_to_speech.voiceId = os.getenv('POLLY_VOICE_ID')
    mqtt_to_speech.engine = os.getenv('POLLY_ENGINE')
    mqtt_to_speech.outputFormat = os.getenv('POLLY_OUTPUT_FORMAT')

    mqtt_to_speech.log_level = int(os.getenv('LOG_LEVEL'))
    mqtt_to_speech.use_redis = bool(int(os.getenv('USE_REDIS')))

    try:
        mqtt_to_speech.start_runner()
    except KeyboardInterrupt:
        mqtt_to_speech.log_info("Exiting.")
        mqtt_to_speech.kill_runner()
    finally:
        mqtt_to_speech.log_info("End.")


if __name__ == "__main__":
    main()
